GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(
    xxhash.go
    xxhash_asm.go
    xxhash_safe.go
)

GO_TEST_SRCS(xxhash_test.go)

IF (ARCH_X86_64)
    SRCS(xxhash_amd64.s)
ENDIF()

IF (ARCH_ARM64)
    SRCS(xxhash_arm64.s)
ENDIF()

END()

RECURSE(gotest)
