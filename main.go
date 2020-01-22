package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

var (
	numConns     int
	numReaders   int
	keyspaceName string
	tableName    string
	cdcTableName string

	testDuration int
	endpoint     string
	rowSize      int
)

type stats struct {
	rowsWritten             uint64
	readQueriesExecuted     uint64
	cumulativeWriteDuration uint64
	cumulativeReadDuration  uint64
}

type Stream struct {
	StreamID1, StreamID2 int64
}

func main() {
	flag.IntVar(&numConns, "connections", 4, "number of connections")
	flag.IntVar(&numReaders, "readers", 8, "number of readers")
	flag.StringVar(&keyspaceName, "keyspace", "keyspace1", "keyspace name")
	flag.StringVar(&tableName, "table", "standard1", "table name")
	flag.IntVar(&testDuration, "duration", 10, "test duration, in seconds")
	flag.StringVar(&endpoint, "endpoint", "127.0.0.1", "endpoint")
	flag.IntVar(&rowSize, "row", 200, "size of the row data to be written")

	flag.Parse()

	cdcTableName = tableName + "_scylla_cdc_log"

	cluster := gocql.NewCluster(endpoint)
	cluster.NumConns = numConns
	cluster.PageSize = 1024
	cluster.Consistency = gocql.Quorum
	cluster.Compressor = &gocql.SnappyCompressor{}
	cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	cluster.Timeout = 3 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	stopper := uint64(0)

	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt)
	go func() {
		<-interrupted
		fmt.Println("\ninterrupted")
		atomic.StoreUint64(&stopper, 1)

		<-interrupted
		fmt.Println("\nkilled")
		os.Exit(1)
	}()

	// Get current streams
	iter := session.Query("SELECT * FROM system_distributed.cdc_description").Iter()
	var timestamp time.Time
	var expired time.Time
	var streams []Stream
	for iter.Scan(&timestamp, &expired, &streams) {
		// fmt.Printf("streams: %#v\n", streams)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	st := &stats{}

	wg := &sync.WaitGroup{}

	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			localStats := stats{}

			defer wg.Done()
			query := session.Query("SELECT \"_C0\" FROM " + keyspaceName + "." + cdcTableName +
				" WHERE stream_id_1 = ? AND stream_id_2 = ? ORDER BY time DESC LIMIT 1")

			r := rand.New(rand.NewSource(rand.Int63()))

			for atomic.LoadUint64(&stopper) == 0 {

				// Choose random stream
				n := r.Intn(len(streams))
				stream := streams[n]

				bound := query.Bind(stream.StreamID1, stream.StreamID2)
				start := time.Now()
				iter := bound.Iter()
				var x, y int
				var c0 []byte
				for iter.Scan(&x, &c0, &y) {
				}
				if err := iter.Close(); err != nil {
					log.Printf("error: %s", err.Error())
				}
				end := time.Now()

				localStats.readQueriesExecuted++
				localStats.cumulativeReadDuration += uint64(end.Sub(start))
			}

			atomic.AddUint64(&st.readQueriesExecuted, localStats.readQueriesExecuted)
			atomic.AddUint64(&st.cumulativeReadDuration, localStats.cumulativeReadDuration)
		}()
	}

	<-time.After(time.Duration(testDuration) * time.Second)
	atomic.StoreUint64(&stopper, 1)
	wg.Wait()

	log.Printf("rows written: %d\n", st.rowsWritten)
	log.Printf("rows per second: %f row/s\n", float64(st.rowsWritten)/float64(testDuration))
	log.Printf("avg query time: %f ms\n", float64(st.cumulativeWriteDuration)/float64(st.rowsWritten)/float64(time.Millisecond))

	log.Printf("reads performed: %d\n", st.readQueriesExecuted)
	log.Printf("reads per second: %f row/s\n", float64(st.readQueriesExecuted)/float64(testDuration))
	log.Printf("avg query time: %f ms\n", float64(st.cumulativeReadDuration)/float64(st.readQueriesExecuted)/float64(time.Millisecond))
}
