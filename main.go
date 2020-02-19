package main

import (
	"flag"
	"fmt"
	"log"
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
)

type Stats struct {
	RequestLatency *hdrhistogram.Histogram

	RowsRead  uint64
	PollsDone uint64
	IdlePolls uint64
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
}

type Stream struct {
	StreamID1, StreamID2 int64
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

	flag.DurationVar(&backoffMinimum, "backoff-min", 10*time.Millisecond, "minimum time to wait on backoff")
	flag.DurationVar(&backoffMaximum, "backoff-max", 500*time.Millisecond, "maximum time to wait on backoff")
	flag.Float64Var(&backoffMultiplier, "backoff-multiplier", 2.0, "multiplier that increases the wait time for consecutive backoffs (must be > 1)")

	flag.DurationVar(&gracePeriod, "grace-period", 100*time.Millisecond, "queries only for log writes older than (now - grace-period), helps mitigate issues with client timestamps")

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
	fmt.Printf("num rows read: %d\n", stats.RowsRead)
	fmt.Printf("rows read/s: %f/s\n", float64(stats.RowsRead)/testDuration.Seconds())
	fmt.Printf("polls/s: %f/s\n", float64(stats.PollsDone)/testDuration.Seconds())
	fmt.Printf("idle polls: %d/%d (%f%%)\n", stats.IdlePolls, stats.PollsDone, 100.0*float64(stats.IdlePolls)/float64(stats.PollsDone))
	fmt.Println()
	fmt.Println("Request latency:")
	printStatsFor(stats.RequestLatency)
}

func printStatsFor(h *hdrhistogram.Histogram) {
	fmt.Printf("  min:    %f ms\n", float64(h.Min())/1000000.0)
	fmt.Printf("  avg:    %f ms\n", h.Mean()/1000000.0)
	fmt.Printf("  median: %f ms\n", float64(h.ValueAtQuantile(50.0))/1000000.0)
	fmt.Printf("  90%%:    %f ms\n", float64(h.ValueAtQuantile(90.0))/1000000.0)
	fmt.Printf("  99%%:    %f ms\n", float64(h.ValueAtQuantile(99.0))/1000000.0)
	fmt.Printf("  99.9%%:  %f ms\n", float64(h.ValueAtQuantile(99.9))/1000000.0)
	fmt.Printf("  max:    %f ms\n", float64(h.Max())/1000000.0)
}

func ReadCdcLog(stop <-chan struct{}, session *gocql.Session, cdcLogTableName string) <-chan *Stats {
	startTimestamp := time.Now()

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
	for _, stream := range bestStreams {
		c := processStream(stop, session, stream, cdcLogTableName, startTimestamp)
		joinChans = append(joinChans, c)
	}
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

		lastTimestamp := gocql.MinTimeUUID(timestamp)
		backoffTime := backoffMinimum

		bypassString := ""
		if bypassCache {
			bypassString = " BYPASS CACHE"
		}
		queryString := fmt.Sprintf(
			"SELECT * FROM %s WHERE stream_id_1 = %d AND stream_id_2 = %d AND time > ? AND time < ?%s",
			cdcLogTableName, stream.StreamID1, stream.StreamID2, bypassString,
		)
		query := session.Query(queryString)

		for {
			select {
			case <-stop:
				return
			default:
			}

			readStart := time.Now()
			iter := query.Bind(lastTimestamp, gocql.MinTimeUUID(time.Now().Add(-gracePeriod))).Iter()

			rowCount := 0
			timestamp := gocql.TimeUUID()

			for {
				data := map[string]interface{}{
					"time": &timestamp,
				}
				if !iter.MapScan(data) {
					break
				}
				rowCount++

				stats.RowsRead++
				lastTimestamp = timestamp
			}
			readEnd := time.Now()

			if err := iter.Close(); err != nil {
				// Log error and continue to backoff logic
				log.Println(err)
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
