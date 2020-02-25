module github.com/piodul/cdc-stressor

go 1.13

require (
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/gocql/gocql v0.0.0-20200115135732-617765adbe2d
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.0.1
