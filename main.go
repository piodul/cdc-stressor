package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"

	"github.com/gocql/gocql"
)

const (
	cdcTableSuffix string = "_scylla_cdc_log"
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

	backoffMinimum    time.Duration
	backoffMaximum    time.Duration
	backoffMultiplier float64

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

	workerID    int
	workerCount int

	verbose bool
)

type Stats struct {
	RequestLatency *hdrhistogram.Histogram

	RowsRead  uint64
	PollsDone uint64
	IdlePolls uint64
	Errors    uint64
}

func NewStats() *Stats {
	return &Stats{
		RequestLatency: hdrhistogram.New(time.Microsecond.Nanoseconds()*50, (timeout + timeout*2).Nanoseconds(), 3),
	}
}

func (stats *Stats) Merge(other *Stats) {
	stats.RequestLatency.Merge(other.RequestLatency)
	stats.RowsRead += other.RowsRead
	stats.PollsDone += other.PollsDone
	stats.IdlePolls += other.IdlePolls
	stats.Errors += other.Errors
}

type Stream []byte

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

	flag.DurationVar(&backoffMinimum, "backoff-min", 10*time.Millisecond, "minimum time to wait on backoff")
	flag.DurationVar(&backoffMaximum, "backoff-max", 500*time.Millisecond, "maximum time to wait on backoff")
	flag.Float64Var(&backoffMultiplier, "backoff-multiplier", 2.0, "multiplier that increases the wait time for consecutive backoffs (must be > 1)")

	flag.DurationVar(&gracePeriod, "grace-period", 100*time.Millisecond, "queries only for log writes older than (now - grace-period), helps mitigate issues with client timestamps")

	flag.DurationVar(&processingTimePerRow, "processing-time-per-row", 10*time.Millisecond, "how much processing time one row adds to current batch")
	flag.Uint64Var(&processingBatchSize, "processing-batch-size", 0, "maximum count of rows to process in one batch; after each batch the goroutine will sleep some time proportional to the number of rows in batch")

	flag.IntVar(&workerID, "worker-id", 0, "id of this worker, used when running multiple instances of this tool; each instance should have a different id, and it must be in range [0..N-1], where N is the number of workers")
	flag.IntVar(&workerCount, "worker-count", 1, "number of workers reading from the same table")

	flag.BoolVar(&verbose, "verbose", false, "enables printing error message each time a read operation on cdc log table fails")

	flag.Parse()

	if !strings.HasSuffix(tableName, cdcTableSuffix) {
		log.Fatalf("table name should have %s suffix", cdcTableSuffix)
	}

	if backoffMinimum > backoffMaximum {
		log.Fatal("minimum backoff time must not be larget than maximum backoff time")
	}

	if backoffMultiplier <= 1.0 {
		log.Fatal("backoff multiplier must be greater than 1")
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

	statsC := ReadCdcLog(stopC, session, keyspaceName+"."+tableName)

	var timeoutC <-chan time.Time
	if testDuration > 0 {
		timeoutC = time.After(testDuration)
	}

	select {
	case <-timeoutC:
	case <-stopC:
	}

	cancel()

	stats := <-statsC
	printFinalResults(stats)
}

func printFinalResults(stats *Stats) {
	fmt.Println("Results:")
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
}

func ReadCdcLog(stop <-chan struct{}, session *gocql.Session, cdcLogTableName string) <-chan *Stats {
	// Account for grace period, so that we won't poll unnecessarily in the beginning
	startTimestamp := time.Now().Add(-gracePeriod)

	// Choose the most recent generation
	iter := session.Query("SELECT time, expired, streams FROM system_distributed.cdc_description BYPASS CACHE").Iter()

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

	ret := make(chan *Stats)

	joinChans := make([]<-chan *Stats, 0)
	for i := workerID; i < len(bestStreams); i += workerCount {
		stream := bestStreams[i]
		c := processStream(stop, session, stream, cdcLogTableName, startTimestamp)
		joinChans = append(joinChans, c)
	}
	log.Printf("Watching changes from %d of %d total streams", len(joinChans), len(bestStreams))
	go func() {
		stats := NewStats()
		for _, c := range joinChans {
			stats.Merge(<-c)
		}
		ret <- stats
	}()

	return ret
}

func processStream(stop <-chan struct{}, session *gocql.Session, stream Stream, cdcLogTableName string, timestamp time.Time) <-chan *Stats {
	ret := make(chan *Stats)

	go func() {
		stats := NewStats()
		defer func() { ret <- stats }()

		lastTimestamp := gocql.UUIDFromTime(timestamp)
		backoffTime := backoffMinimum

		bypassString := ""
		if bypassCache {
			bypassString = " BYPASS CACHE"
		}
		queryString := fmt.Sprintf(
			"SELECT * FROM %s WHERE \"cdc$stream_id\" = ? AND \"cdc$time\" > ? AND \"cdc$time\" < ?%s",
			cdcLogTableName, bypassString,
		)
		query := session.Query(queryString)

		for {
			select {
			case <-stop:
				return
			default:
			}

			readStart := time.Now()
			iter := query.Bind(stream, lastTimestamp, gocql.UUIDFromTime(time.Now().Add(-gracePeriod))).Iter()

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

				stats.RowsRead++
				lastTimestamp = timestamp

				if batchRowCount == processingBatchSize {
					if doStop := sleepForBatch(); doStop {
						break
					}
				}
			}

			readEnd := time.Now()

			if err := iter.Close(); err != nil {
				// Log error and continue to backoff logic
				if verbose {
					log.Println(err)
				}
				stats.Errors++
			} else {
				stats.RequestLatency.RecordValue(readEnd.Sub(readStart).Nanoseconds())
			}
			stats.PollsDone++

			if rowCount == 0 {
				stats.IdlePolls++
				select {
				case <-time.After(backoffTime):
					backoffTime *= time.Duration(float64(backoffTime) * backoffMultiplier)
					if backoffTime > backoffMaximum {
						backoffTime = backoffMaximum
					}
				case <-stop:
					return
				}
			} else {
				backoffTime = backoffMinimum
			}
		}
	}()

	return ret
}
