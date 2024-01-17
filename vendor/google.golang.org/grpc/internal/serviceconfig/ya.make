GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    duration.go
    serviceconfig.go
)

GO_TEST_SRCS(
    duration_test.go
    serviceconfig_test.go
)

END()

RECURSE(gotest)
