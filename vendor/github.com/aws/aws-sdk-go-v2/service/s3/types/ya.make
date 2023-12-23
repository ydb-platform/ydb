GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    enums.go
    errors.go
    types.go
)

GO_XTEST_SRCS(types_exported_test.go)

END()

RECURSE(
    gotest
)
