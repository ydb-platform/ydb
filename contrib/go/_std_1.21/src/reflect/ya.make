GO_LIBRARY()

SRCS(
    abi.go
    deepequal.go
    float32reg_generic.go
    makefunc.go
    swapper.go
    type.go
    value.go
    visiblefields.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    abi_test.go
    all_test.go
    benchmark_test.go
    example_test.go
    nih_test.go
    set_test.go
    tostring_test.go
    visiblefields_test.go
)

IF (ARCH_X86_64)
    SRCS(
        asm_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        asm_arm64.s
    )
ENDIF()

END()

RECURSE(
    internal
)
