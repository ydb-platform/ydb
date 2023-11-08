GO_LIBRARY()

SRCS(
    middleware.go
    middleware_opts.go
)

GO_TEST_SRCS(middleware_test.go)

END()

RECURSE(gotest)
