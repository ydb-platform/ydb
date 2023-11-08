GO_LIBRARY()

SRCS(
    histogram.go
    number.go
    registry.go
    tags.go
    unistat.go
)

GO_TEST_SRCS(
    histogram_test.go
    number_test.go
    registry_test.go
    tags_test.go
    unistat_test.go
)

END()

RECURSE(
    aggr
    example_server
    gotest
)
