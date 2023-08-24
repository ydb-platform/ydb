GO_LIBRARY()

SRCS(
    fields.go
    levels.go
    log.go
)

GO_TEST_SRCS(fields_test.go)

GO_XTEST_SRCS(levels_test.go)

END()

RECURSE(
    compat
    ctxlog
    gotest
    nop
    test
    zap
)
