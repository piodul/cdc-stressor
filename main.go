package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"
	"golang.org/x/sync/semaphore"

	"github.com/gocql/gocql"
)

const (
	cdcTableSuffix string = "_scylla_cdc_log"
)

var (
	// The table that keeps names of the generations changed names.
	// This is a list of all supported names, starting from the newest one.
	generationsTableNames []string = []string{
		"system_distributed.cdc_streams_descriptions", // Introduced in Scylla 4.3
		"system_distributed.cdc_streams",              // Introduced in Scylla 4.1
		"system_distributed.cdc_description",
	}
)

var (
	numConns     int
	keyspaceName string
	tableName    string
	timeout      time.Duration

	testDuration      time.Duration
	nodes             string
	pageSize          int
	consistencyLevel  string
	clientCompression bool
	bypassCache       bool

	// Streams are queried in round-robin fashion. This parameter specifies the length
	// of one full round (in one round, all streams are queried).
	streamQueryRoundDuration time.Duration

	// Allows for tracking streams in groups. If this value is more than 1, one goroutine
	// tracks `groupSize` streams, and fetches data from all its streams using one query.
	groupSize int

	// If client timestamps are used, it might result in rows with older timestamps
	// than the last row to be inserted into a stream. If we just polled for rows
	// newer than the timestamp of the last received rows, it would cause some rows
	// to be missed.
	// This option helps to mitigate that issue by querying for rows that are
	// older than (now - `gracePeriod`) timestamp.`
	gracePeriod time.Duration

	// After fetching at most processingBatchSize rows, processing of the rows
	// will be simulated by sleeping for processingBatchSize * processingTimePerRow.
	processingTimePerRow time.Duration
	processingBatchSize  uint64

	maxConcurrentPolls int64

	logInterval time.Duration

	workerID    int
	workerCount int

	verbose                bool
	printPollSizeHistogram bool
)

type Stats struct {
	RequestLatency *hdrhistogram.Histogram

	TimeElapsed time.Duration
	RowsRead    uint64
	PollsDone   uint64
	IdlePolls   uint64
	Errors      uint64

	PollSizeDistribution map[int]int

	Final bool
}

func NewStats() *Stats {
	stats := &Stats{
		RequestLatency: hdrhistogram.New(time.Microsecond.Nanoseconds()*50, (timeout + timeout*2).Nanoseconds(), 3),
	}

	if printPollSizeHistogram {
		stats.PollSizeDistribution = make(map[int]int)
	}

	return stats
}

func (stats *Stats) Merge(other *Stats) {
	if stats.TimeElapsed < other.TimeElapsed {
		stats.TimeElapsed = other.TimeElapsed
	}
	stats.RequestLatency.Merge(other.RequestLatency)
	stats.RowsRead += other.RowsRead
	stats.PollsDone += other.PollsDone
	stats.IdlePolls += other.IdlePolls
	stats.Errors += other.Errors

	if printPollSizeHistogram {
		for pollSize, count := range other.PollSizeDistribution {
			stats.PollSizeDistribution[pollSize] += count
		}
	}
}

type Stream []byte

func getFlagToParsedStateMap() map[string]bool {
	parsedFlags := make(map[string]bool)
	// Visit all flags and set to false
	flag.VisitAll(func(f *flag.Flag) {
		parsedFlags[f.Name] = false
	})
	// Visit only parsed flags and set to true
	flag.Visit(func(f *flag.Flag) {
		parsedFlags[f.Name] = true
	})
	return parsedFlags
}

func main() {
	flag.IntVar(&numConns, "connection-count", 4, "number of connections")
	flag.StringVar(&keyspaceName, "keyspace", "scylla_bench", "keyspace name")
	flag.StringVar(&tableName, "table", "test"+cdcTableSuffix, "name of the cdc table to read from")
	flag.DurationVar(&testDuration, "duration", 0, "test duration, value <= 0 makes the test run infinitely until stopped")
	flag.StringVar(&nodes, "nodes", "127.0.0.1", "cluster nodes to connect to")
	flag.IntVar(&pageSize, "page-size", 1000, "page size")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "request timeout")
	flag.StringVar(&consistencyLevel, "consistency-level", "quorum", "consistency level to use when reading")
	flag.BoolVar(&clientCompression, "client-compression", true, "use compression for client-coordinator communication")
	flag.BoolVar(&bypassCache, "bypass-cache", true, "use BYPASS CACHE when querying the cdc log table")

	flag.DurationVar(&streamQueryRoundDuration, "stream-query-round-duration", 1*time.Second, "specifies the length of one full round of querying all streams")

	flag.IntVar(&groupSize, "group-size", 1, "how many streams should one goroutine track at once")

	flag.DurationVar(&gracePeriod, "grace-period", 1*time.Second, "queries only for log writes older than (now - grace-period), helps mitigate issues with client timestamps")

	flag.DurationVar(&processingTimePerRow, "processing-time-per-row", 0, "how much processing time one row adds to current batch")
	flag.Uint64Var(&processingBatchSize, "processing-batch-size", 0, "maximum count of rows to process in one batch; after each batch the goroutine will sleep some time proportional to the number of rows in batch")

	flag.Int64Var(&maxConcurrentPolls, "max-concurrent-polls", 500, "maximum number of polls happening at the same time")

	flag.IntVar(&workerID, "worker-id", 0, "id of this worker, used when running multiple instances of this tool; each instance should have a different id, and it must be in range [0..N-1], where N is the number of workers")
	flag.IntVar(&workerCount, "worker-count", 1, "number of workers reading from the same table")

	flag.DurationVar(&logInterval, "log-interval", time.Second, "how much time to wait between printing partial results")
	flag.BoolVar(&verbose, "verbose", false, "enables printing error message each time a read operation on cdc log table fails")
	flag.BoolVar(&printPollSizeHistogram, "print-poll-size-histogram", true, "enables printing poll size histogram at the end")

	// Deprecated options
	backoffMinDeprecated := flag.Duration("backoff-min", time.Second, "(deprecated) polling method has changed, please use stream-query-round-duration instead")
	backoffMaxDeprecated := flag.Duration("backoff-max", time.Second, "(deprecated) polling method has changed, please use stream-query-round-duration instead")

	flag.Parse()

	flagWasParsed := getFlagToParsedStateMap()

	if !flagWasParsed["stream-query-round-duration"] {
		if flagWasParsed["backoff-min"] || flagWasParsed["backoff-max"] {
			streamQueryRoundDuration = (*backoffMinDeprecated + *backoffMaxDeprecated) / time.Duration(2)
		}
	}

	if !strings.HasSuffix(tableName, cdcTableSuffix) {
		log.Fatalf("table name should have %s suffix", cdcTableSuffix)
	}

	if groupSize <= 0 {
		log.Fatal("group size must be positive")
	}

	if workerCount < 1 {
		log.Fatal("worker count must be larger than 0")
	}

	if workerID < 0 || workerID >= workerCount {
		log.Fatal("worker id must be from range [0..N-1], where N is the number of workers")
	}

	if processingBatchSize == 0 {
		processingBatchSize = math.MaxInt64
	}

	if maxConcurrentPolls <= 0 {
		log.Fatal("you must allow at least one concurrent poll to happen at the same time")
	}

	cluster := gocql.NewCluster(strings.Split(nodes, ",")...)
	cluster.NumConns = numConns
	cluster.PageSize = pageSize
	cluster.Compressor = &gocql.SnappyCompressor{}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Timeout = timeout

	switch consistencyLevel {
	case "any":
		cluster.Consistency = gocql.Any
	case "one":
		cluster.Consistency = gocql.One
	case "two":
		cluster.Consistency = gocql.Two
	case "three":
		cluster.Consistency = gocql.Three
	case "quorum":
		cluster.Consistency = gocql.Quorum
	case "all":
		cluster.Consistency = gocql.All
	case "local_quorum":
		cluster.Consistency = gocql.LocalQuorum
	case "each_quorum":
		cluster.Consistency = gocql.EachQuorum
	case "local_one":
		cluster.Consistency = gocql.LocalOne
	default:
		log.Fatalf("unknown consistency level: %s", consistencyLevel)
	}
	if clientCompression {
		cluster.Compressor = &gocql.SnappyCompressor{}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	stopC := make(chan struct{})

	o := &sync.Once{}
	cancel := func() {
		o.Do(func() { close(stopC) })
	}

	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt)
	go func() {
		<-interrupted
		log.Println("interrupted")
		cancel()

		<-interrupted
		log.Println("killed")
		os.Exit(1)
	}()

	finished := ReadCdcLog(stopC, session, keyspaceName+"."+tableName)

	var timeoutC <-chan time.Time
	if testDuration > 0 {
		timeoutC = time.After(testDuration)
	}

	select {
	case <-timeoutC:
	case <-stopC:
	}

	cancel()

	<-finished
}

func printPartialResults(stats *Stats, normalizationFactor float64) {
	fmtString := "%-15v  %15v  %7v    %7v\n"
	normalized := func(i uint64) uint64 {
		return uint64(math.Ceil(float64(i) * normalizationFactor))
	}
	fmt.Printf(fmtString, stats.TimeElapsed, normalized(stats.PollsDone), normalized(stats.RowsRead), normalized(stats.Errors))
}

func printFinalResults(stats *Stats) {
	fmt.Println("\nResults:")
	fmt.Printf("num rows read:  %d\n", stats.RowsRead)
	fmt.Printf("rows read/s:    %f/s\n", float64(stats.RowsRead)/testDuration.Seconds())
	fmt.Printf("polls/s:        %f/s\n", float64(stats.PollsDone)/testDuration.Seconds())
	fmt.Printf("idle polls:     %d/%d (%f%%)\n", stats.IdlePolls, stats.PollsDone, 100.0*float64(stats.IdlePolls)/float64(stats.PollsDone))
	fmt.Printf("errors:         %d\n", stats.Errors)
	fmt.Printf("latency min:    %f ms\n", float64(stats.RequestLatency.Min())/1000000.0)
	fmt.Printf("latency avg:    %f ms\n", stats.RequestLatency.Mean()/1000000.0)
	fmt.Printf("latency median: %f ms\n", float64(stats.RequestLatency.ValueAtQuantile(50.0))/1000000.0)
	fmt.Printf("latency 90%%:    %f ms\n", float64(stats.RequestLatency.ValueAtQuantile(90.0))/1000000.0)
	fmt.Printf("latency 99%%:    %f ms\n", float64(stats.RequestLatency.ValueAtQuantile(99.0))/1000000.0)
	fmt.Printf("latency 99.9%%:  %f ms\n", float64(stats.RequestLatency.ValueAtQuantile(99.9))/1000000.0)
	fmt.Printf("latency max:    %f ms\n", float64(stats.RequestLatency.Max())/1000000.0)

	if printPollSizeHistogram {
		pollSizes := make([]int, 0, len(stats.PollSizeDistribution))
		for pollSize := range stats.PollSizeDistribution {
			pollSizes = append(pollSizes, pollSize)
		}
		sort.Ints(pollSizes)

		fmt.Println("\npoll size distribution:")
		fmt.Println("  size   :   count")
		for _, pollSize := range pollSizes {
			fmt.Printf("  %-7d: %7d\n", pollSize, stats.PollSizeDistribution[pollSize])
		}
	}
}

func getGenerationsTableName(session *gocql.Session) string {
	for _, name := range generationsTableNames {
		splitName := strings.Split(name, ".")
		keyspaceName := splitName[0]
		tableName := splitName[1]

		iter := session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspaceName, tableName)).Iter()
		var count int
		iter.Scan(&count)

		if err := iter.Close(); err != nil {
			fmt.Printf("%#v\n", err)
			log.Fatal(err)
		}

		if count == 1 {
			return name
		}
	}

	log.Fatal("there is no table to read cdc generations from")
	return ""
}

func ReadCdcLog(stop <-chan struct{}, session *gocql.Session, cdcLogTableName string) <-chan struct{} {
	// Account for grace period, so that we won't poll unnecessarily in the beginning
	startTimestamp := time.Now().Add(-gracePeriod)

	// Choose the most recent generation
	iter := session.Query(fmt.Sprintf("SELECT time, expired, streams FROM %s BYPASS CACHE", getGenerationsTableName(session))).Iter()

	var timestamp, bestTimestamp, expired time.Time
	var streams, bestStreams []Stream

	for iter.Scan(&timestamp, &expired, &streams) {
		if bestTimestamp.Before(timestamp) {
			bestTimestamp = timestamp
			bestStreams = streams
		}
	}

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	if len(bestStreams) == 0 {
		log.Fatal("There are no streams in the most recent generation, or there are no generations in cdc_description table")
	}

	concurrencySem := semaphore.NewWeighted(maxConcurrentPolls)
	finished := make(chan struct{})

	// Split into groups
	streamGroups := make([][]Stream, 0)
	var currentGroup []Stream
	for _, stream := range bestStreams {
		currentGroup = append(currentGroup, stream)
		if len(currentGroup) >= groupSize {
			streamGroups = append(streamGroups, currentGroup)
			currentGroup = nil
		}
	}
	if len(currentGroup) > 0 {
		streamGroups = append(streamGroups, currentGroup)
		currentGroup = nil
	}

	statsChans := make([]<-chan *Stats, 0)
	canAdvanceChans := make([]chan struct{}, 0)
	trackedStreamsCount := 0
	for i := workerID; i < len(streamGroups); i += workerCount {
		streams := streamGroups[i]
		trackedStreamsCount += len(streams)
		workerAdvanceChan := make(chan struct{}, 1)
		canAdvanceChans = append(canAdvanceChans, workerAdvanceChan)
		c := processStreams(stop, workerAdvanceChan, session, concurrencySem, streams, cdcLogTableName, startTimestamp)
		statsChans = append(statsChans, c)
	}
	log.Printf("Watching changes from %d stream groups (%d streams of %d total)", len(statsChans), trackedStreamsCount, len(bestStreams))
	fmt.Println("Time elapsed             Polls/s   Rows/s   Errors/s")

	localWorkerCount := len(statsChans)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stop
		cancel()
	}()

	go func() {
		interval := streamQueryRoundDuration / time.Duration(localWorkerCount)
		currentWorkerID := 0

		for {
			select {
			case <-stop:
				return
			default:
			}

			if err := concurrencySem.Acquire(ctx, 1); err != nil {
				return
			}

			canAdvanceChans[currentWorkerID] <- struct{}{}

			currentWorkerID = (currentWorkerID + 1) % localWorkerCount
			time.Sleep(interval)
		}
	}()

	// Stats collector
	go func() {
		previousTimeElapsed := time.Duration(0)
		for {
			final, stats := mergeStats(statsChans)
			if final {
				printFinalResults(stats)
				finished <- struct{}{}
				return
			} else {
				normalizationFactor := float64(time.Second) / float64(stats.TimeElapsed-previousTimeElapsed)
				printPartialResults(stats, normalizationFactor)
				previousTimeElapsed = stats.TimeElapsed
				continue
			}
		}
	}()

	return finished
}

func mergeStats(statsChans []<-chan *Stats) (final bool, result *Stats) {
	result = NewStats()
	for i, ch := range statsChans {
		streamStats := <-ch
		if streamStats.Final {
			result = streamStats
			result = mergeFinalStats(result, statsChans[:i])
			result = mergeFinalStats(result, statsChans[i+1:])
			return true, result
		}

		result.Merge(streamStats)
	}

	return false, result
}

func mergeFinalStats(result *Stats, statsChans []<-chan *Stats) *Stats {
	for _, ch := range statsChans {
		streamStats := <-ch
		for !streamStats.Final {
			streamStats = <-ch
		}
		result.Merge(streamStats)
	}
	return result
}

func processStreams(stop <-chan struct{}, canAdvance <-chan struct{}, session *gocql.Session, concurrencySem *semaphore.Weighted, streams []Stream, cdcLogTableName string, timestamp time.Time) <-chan *Stats {
	statsChan := make(chan *Stats, 1)

	go func() {
		processStartTime := time.Now()
		nextReportTime := processStartTime

		finalStats := NewStats()
		finalStats.Final = true
		currentStats := NewStats()
		defer func() {
			finalStats.Merge(currentStats)
			statsChan <- finalStats
		}()

		lastTimestamp := gocql.UUIDFromTime(timestamp)

		bypassString := ""
		if bypassCache {
			bypassString = " BYPASS CACHE"
		}
		queryString := fmt.Sprintf(
			"SELECT * FROM %s WHERE \"cdc$stream_id\" IN (%s) AND \"cdc$time\" > ? AND \"cdc$time\" < ?%s",
			cdcLogTableName,
			"?"+strings.Repeat(", ?", len(streams)-1),
			bypassString,
		)
		query := session.Query(queryString)

		for {
			select {
			case <-canAdvance:
			case <-stop:
				return
			}

			readStart := time.Now()
			bindArgs := make([]interface{}, 0, len(streams)+2)
			for _, stream := range streams {
				bindArgs = append(bindArgs, stream)
			}
			bindArgs = append(bindArgs, lastTimestamp, gocql.UUIDFromTime(time.Now().Add(-gracePeriod)))
			iter := query.Bind(bindArgs...).Iter()

			rowCount := 0
			batchRowCount := uint64(0)
			timestamp := gocql.TimeUUID()

			sleepForBatch := func() (doStop bool) {
				sleepDuration := time.Duration(batchRowCount) * processingTimePerRow
				batchRowCount = 0
				select {
				case <-stop:
					return true
				case <-time.After(sleepDuration):
					return false
				}
			}

			for {
				data := map[string]interface{}{
					"cdc$time": &timestamp,
				}
				if !iter.MapScan(data) {
					sleepForBatch()
					break
				}
				rowCount++
				batchRowCount++

				currentStats.RowsRead++
				if bytes.Compare(lastTimestamp[:], timestamp[:]) < 0 {
					lastTimestamp = timestamp
				}

				if batchRowCount == processingBatchSize {
					if doStop := sleepForBatch(); doStop {
						break
					}
				}
			}

			readEnd := time.Now()
			concurrencySem.Release(1)

			if err := iter.Close(); err != nil {
				// Log error and continue to backoff logic
				if verbose {
					log.Println(err)
				}
				currentStats.Errors++
			} else {
				// Adjust for the sleeping time
				latency := readEnd.Sub(readStart)
				latency -= time.Duration(rowCount) * processingTimePerRow
				currentStats.RequestLatency.RecordValue(readEnd.Sub(readStart).Nanoseconds())
			}
			currentStats.PollsDone++
			if printPollSizeHistogram {
				currentStats.PollSizeDistribution[rowCount]++
			}

			if rowCount == 0 {
				currentStats.IdlePolls++
			}

			now := time.Now()
			if now.After(nextReportTime) {
				currentStats.TimeElapsed = now.Sub(processStartTime)
				finalStats.Merge(currentStats)
				statsChan <- currentStats

				currentStats = NewStats()
				nextReportTime = nextReportTime.Add(logInterval)
			}
		}
	}()

	return statsChan
}
