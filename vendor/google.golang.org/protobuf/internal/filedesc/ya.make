GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    build.go
    desc.go
    desc_init.go
    desc_lazy.go
    desc_list.go
    desc_list_gen.go
    placeholder.go
)

GO_XTEST_SRCS(
    build_test.go
    desc_test.go
)

END()

RECURSE(gotest)
