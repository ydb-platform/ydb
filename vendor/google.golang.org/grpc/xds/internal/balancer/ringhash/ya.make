GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    config.go
    logging.go
    picker.go
    ring.go
    ringhash.go
    util.go
)

GO_TEST_SRCS(
    config_test.go
    picker_test.go
    ring_test.go
    ringhash_test.go
)

END()

RECURSE(
    e2e
    gotest
)
