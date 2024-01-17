GO_LIBRARY()

SRCS(
    doc.go
    stubs.go
    types.go
    types_64bit.go
    unaligned.go
)

GO_XTEST_SRCS(
    atomic_test.go
    bench_test.go
)

IF (ARCH_X86_64)
    SRCS(
        atomic_amd64.go
        atomic_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        atomic_arm64.go
        atomic_arm64.s
    )
ENDIF()

END()

RECURSE(
)
