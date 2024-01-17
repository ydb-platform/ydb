GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    dynamic.go
    types.go
)

GO_XTEST_SRCS(
    dynamic_test.go
    types_test.go
)

END()

RECURSE(gotest)
