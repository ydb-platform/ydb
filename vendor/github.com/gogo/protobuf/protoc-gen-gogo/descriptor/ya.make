GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.3.2)

SRCS(
    descriptor.go
    descriptor.pb.go
    descriptor_gostring.gen.go
    helper.go
)

GO_XTEST_SRCS(descriptor_test.go)

END()

RECURSE(
    gotest
)
