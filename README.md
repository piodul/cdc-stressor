# CDC stressor

cdc-stressor is a simple tool for testing read workloads CDC log tables. It simulates a client that watches changes that appear in a CDC log table.

## How it works

The tool starts with fetching the most recent stream generation from `scylla_distributed.cdc_description`. Then, for each stream, a separate goroutine is spawned.

Each goroutine, in a loop, polls for changes to their associated stream. They do that by requesting rows from their streams starting from the last processed timestamp, with a similar query to the following:

    SELECT * FROM <table name> WHERE stream_id_1 = <id 1> AND stream_id_2 = <id 2> AND time > ? (BYPASS CACHE)

If the query returns any rows, the request is retried immediately with the timestamp of the last row processed. Otherwise, the request is retried with exponential backoff (which is configurable).

This tool fetches all columns from the CDC log table, but ignores all columns apart from the `timestamp` column. This means that it works with any kind of base table schema.

## Usage

By default, cdc-stressor will try to connect to localhost and read from `scylla_bench.test_scylla_cdc_log` indefinitely. You can override the defaults with commandline options, full listing is presented below:

    -bypass-cache
        use BYPASS CACHE when querying the cdc log table (default true)
    -client-compression
        use compression for client-coordinator communication (default true)
    -connection-count int
        number of connections (default 4)
    -consistency-level string
        consistency level to use when reading (default "quorum")
    -duration duration
        test duration, value <= 0 makes the test run infinitely until stopped
    -grace-period duration
        queries only for log writes older than (now - grace-period), helps mitigate issues with client timestamps (default 1s)
    -keyspace string
        keyspace name (default "scylla_bench")
    -log-interval duration
        how much time to wait between printing partial results (default 1s)
    -max-concurrent-polls int
        maximum number of polls happening at the same time (default 500)
    -nodes string
        cluster nodes to connect to (default "127.0.0.1")
    -page-size int
        page size (default 1000)
    -print-poll-size-histogram
        enables printing poll size histogram at the end (default true)
    -processing-batch-size uint
        maximum count of rows to process in one batch; after each batch the goroutine will sleep some time proportional to the number of rows in batch
    -processing-time-per-row duration
        how much processing time one row adds to current batch
    -stream-query-round-duration duration
        specifies the length of one full round of querying all streams (default 1s)
    -table string
        name of the cdc table to read from (default "test_scylla_cdc_log")
    -timeout duration
        request timeout (default 5s)
    -verbose
        enables printing error message each time a read operation on cdc log table fails
    -worker-count int
        number of workers reading from the same table (default 1)
    -worker-id int
        id of this worker, used when running multiple instances of this tool; each instance should have a different id, and it must be in range [0..N-1], where N is the number of workers

The tool can be stopped with SIGINT/Ctrl+C, after which it will print some statistics on the number of requests and request latency.

## Limitations/TODO

This tool does not work when a new CDC generation becomes active. This happens when a new node is joined to the cluster. If you don't change the topology of the cluster during the test, the tool should be working fine.
