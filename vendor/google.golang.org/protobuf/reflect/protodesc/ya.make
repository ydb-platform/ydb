GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    desc.go
    desc_init.go
    desc_resolve.go
    desc_validate.go
    proto.go
)

GO_TEST_SRCS(file_test.go)

END()

RECURSE(gotest)
