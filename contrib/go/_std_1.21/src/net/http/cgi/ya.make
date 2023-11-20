GO_LIBRARY()

SRCS(
    child.go
    host.go
)

GO_TEST_SRCS(
    child_test.go
    host_test.go
    integration_test.go
    posix_test.go
)

END()

RECURSE(
)
