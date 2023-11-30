GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    hscan.go
    structmap.go
)

GO_TEST_SRCS(hscan_test.go)

END()

RECURSE(gotest)
