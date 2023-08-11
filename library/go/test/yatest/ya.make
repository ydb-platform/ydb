GO_LIBRARY()

SRCS(
    arcadia.go
    env.go
    go.go
)

GO_TEST_SRCS(env_test.go)

END()

RECURSE_FOR_TESTS(gotest)
