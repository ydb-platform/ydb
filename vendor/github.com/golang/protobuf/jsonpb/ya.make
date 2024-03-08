GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    decode.go
    encode.go
    json.go
)

GO_TEST_SRCS(json_test.go)

END()

RECURSE(
    gotest
)
