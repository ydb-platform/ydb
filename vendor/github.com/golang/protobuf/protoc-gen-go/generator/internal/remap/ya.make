GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    remap.go
)

GO_TEST_SRCS(remap_test.go)

END()

RECURSE(
    gotest
)
