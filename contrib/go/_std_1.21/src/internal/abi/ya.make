GO_LIBRARY()

SRCS(
    abi.go
    abi_test.s
    compiletype.go
    funcpc.go
    map.go
    stack.go
    stub.s
    symtab.go
    type.go
    unsafestring_go120.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(abi_test.go)

IF (ARCH_X86_64)
    SRCS(
        abi_amd64.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        abi_arm64.go
    )
ENDIF()

END()

RECURSE(
)
