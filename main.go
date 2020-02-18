package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/codahale/hdrhistogram"

	"github.com/gocql/gocql"
)

const (
	minBackoffTime time.Duration = 10 * time.Millisecond
	maxBackoffTime time.Duration = 500 * time.Millisecond
)

var (
	numConns     int
	keyspaceName string
	tableName    string
	cdcTableName string
	timeout      time.Duration

	testDuration time.Duration
	endpoint     string
	rowSize      int
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
	flag.IntVar(&numConns, "connections", 4, "number of connections")
	flag.StringVar(&keyspaceName, "keyspace", "keyspace1", "keyspace name")
	flag.StringVar(&tableName, "table", "standard1", "table name")
	flag.DurationVar(&testDuration, "duration", 10*time.Second, "test duration")
	flag.StringVar(&endpoint, "endpoint", "127.0.0.1", "endpoint")
	flag.IntVar(&rowSize, "row", 200, "size of the row data to be written")
	flag.DurationVar(&timeout, "timeout", 5*time.Second, "request timeout")

	flag.Parse()

	cdcTableName = tableName + "_scylla_cdc_log"

	cluster := gocql.NewCluster(endpoint)
	cluster.NumConns = numConns
	cluster.PageSize = 1024
	cluster.Consistency = gocql.Quorum
	cluster.Compressor = &gocql.SnappyCompressor{}
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Timeout = timeout

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
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
		fmt.Println("\ninterrupted")
		cancel()

		<-interrupted
		fmt.Println("\nkilled")
		os.Exit(1)
	}()

	statsC := ReadCdcLog(stopC, session, keyspaceName+"."+tableName)

	select {
	case <-time.After(testDuration):
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

// TODO: For now, it only reads the last stream set, and does not react to changes to that stream
func ReadCdcLog(stop <-chan struct{}, session *gocql.Session, tableName string) <-chan *Stats {
	startTimestamp := time.Now()
	cdcLogTableName := tableName + "_scylla_cdc_log"

	// Find the last stream set
	iter := session.Query("SELECT time, expired, streams FROM system_distributed.cdc_description BYPASS CACHE").Iter()

	var timestamp, bestTimestamp, expired time.Time
	var streams, bestStreams []Stream

	for iter.Scan(&timestamp, &expired, &streams) {
		if bestTimestamp.Before(timestamp) {
			bestTimestamp = timestamp
			bestStreams = streams
		}
	}

	// There should be at least one stream present...
	if err := iter.Close(); err != nil {
		panic(err)
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
		backoffTime := minBackoffTime

		for {
			select {
			case <-stop:
				return
			default:
			}

			queryString := fmt.Sprintf("SELECT * FROM %s WHERE stream_id_1 = ? AND stream_id_2 = ? AND time > ? BYPASS CACHE", cdcLogTableName)
			readStart := time.Now()
			iter := session.Query(queryString).
				Bind(stream.StreamID1, stream.StreamID2, lastTimestamp).
				Iter()

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
				if err == gocql.ErrTimeoutNoResponse {
					// Tough luck, let's try again later
					continue
				}
				return
			}

			stats.RequestLatency.RecordValue(readEnd.Sub(readStart).Nanoseconds())
			stats.PollsDone++

			if rowCount == 0 {
				stats.IdlePolls++
				select {
				case <-time.After(backoffTime):
					backoffTime *= 2
					if backoffTime > maxBackoffTime {
						backoffTime = maxBackoffTime
					}
				case <-stop:
					return
				}
			}
		}
	}()

	return ret
}
