GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    edf.go
    random.go
    wrr.go
)

GO_TEST_SRCS(
    edf_test.go
    wrr_test.go
)

END()

RECURSE(gotest)
