module github.com/piodul/cdc-stressor

go 1.13

require (
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/gocql/gocql v0.0.0-20200410100145-b454769479c6
	github.com/golang/snappy v0.0.1 // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.3.4
