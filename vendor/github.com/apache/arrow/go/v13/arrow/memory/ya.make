GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-2-Clause AND
    BSD-3-Clause AND
    BSL-1.0 AND
    CC-BY-3.0 AND
    HPND AND
    MIT AND
    NCSA AND
    OpenSSL AND
    Zlib
)

SRCS(
    allocator.go
    buffer.go
    checked_allocator.go
    default_allocator.go
    doc.go
    go_allocator.go
    memory.go
    util.go
)

GO_TEST_SRCS(
    go_allocator_test.go
    util_test.go
)

GO_XTEST_SRCS(
    buffer_test.go
    memory_test.go
)

IF (ARCH_X86_64)
    SRCS(
        memory_amd64.go
        memory_avx2_amd64.go
        memory_avx2_amd64.s
        memory_sse4_amd64.go
        memory_sse4_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        memory_arm64.go
        memory_neon_arm64.go
        memory_neon_arm64.s
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        memory_arm64.go
        memory_neon_arm64.go
        memory_neon_arm64.s
    )
ENDIF()

END()

RECURSE(mallocator)
