GO_LIBRARY()

SRCS(sem.go)

GO_TEST_SRCS(leak_test.go)

END()

RECURSE(
    dummy
    gotest
)
