GO_LIBRARY()

LICENSE(MIT)

SRCS(
    xxhash.go
    xxhash_asm.go
    xxhash_unsafe.go
)

GO_TEST_SRCS(
    bench_test.go
    xxhash_test.go
    xxhash_unsafe_test.go
)

IF (ARCH_X86_64)
    SRCS(xxhash_amd64.s)
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(xxhash_arm64.s)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(xxhash_arm64.s)
ENDIF()

END()

RECURSE(
    # gotest
)
