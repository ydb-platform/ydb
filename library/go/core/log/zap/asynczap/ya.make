GO_LIBRARY()

SRCS(
    background.go
    core.go
    options.go
    queue.go
)

GO_TEST_SRCS(
    core_test.go
    queue_test.go
)

END()

RECURSE(gotest)
