GO_LIBRARY()

SRCS(
    bytealg.go
    compare_native.go
    count_native.go
    equal_generic.go
    equal_native.go
    index_native.go
    indexbyte_native.go
)

IF (ARCH_ARM64)
    SRCS(
        compare_arm64.s
        count_arm64.s
        equal_arm64.s
        index_arm64.go
        index_arm64.s
        indexbyte_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        compare_amd64.s
        count_amd64.s
        equal_amd64.s
        index_amd64.go
        index_amd64.s
        indexbyte_amd64.s
    )
ENDIF()

END()
