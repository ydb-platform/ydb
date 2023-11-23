GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    strings.go
    strings_unsafe.go
)

GO_TEST_SRCS(strings_test.go)

END()

RECURSE(gotest)
