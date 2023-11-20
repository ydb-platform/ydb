GO_LIBRARY()

SRCS(
    asm.s
    swapper.go
    type.go
    value.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    all_test.go
    reflect_mirror_test.go
    set_test.go
    tostring_test.go
)

END()

RECURSE(
)
