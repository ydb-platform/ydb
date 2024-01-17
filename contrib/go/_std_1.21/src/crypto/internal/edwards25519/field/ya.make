GO_LIBRARY()

SRCS(
    fe.go
    fe_generic.go
)

GO_TEST_SRCS(
    fe_alias_test.go
    fe_bench_test.go
    fe_test.go
)

IF (ARCH_X86_64)
    SRCS(
        fe_amd64.go
        fe_amd64.s
        fe_arm64_noasm.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        fe_amd64_noasm.go
        fe_arm64.go
        fe_arm64.s
    )
ENDIF()

END()

RECURSE(
)

RECURSE(
   # _asm
)
