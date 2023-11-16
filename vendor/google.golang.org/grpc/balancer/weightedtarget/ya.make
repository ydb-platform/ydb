GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    logging.go
    weightedtarget.go
    weightedtarget_config.go
)

GO_TEST_SRCS(
    weightedtarget_config_test.go
    weightedtarget_test.go
)

END()

RECURSE(
    gotest
    weightedaggregator
)
