GO_LIBRARY()

SRCS(
    apis.go
    dummy.s
    emit.go
    hooks.go
    testsupport.go
)

GO_TEST_SRCS(
    emitdata_test.go
    ts_test.go
)

END()

RECURSE(
)
