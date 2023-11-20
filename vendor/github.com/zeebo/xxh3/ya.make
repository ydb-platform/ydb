GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    accum_generic.go
    consts.go
    hash128.go
    hash128_seed.go
    hash64.go
    hash64_seed.go
    hasher.go
    utils.go
)

GO_TEST_SRCS(
    compat_test.go
    compat_vector_test.go
    escape_test.go
    hash128_test.go
    hash64_test.go
    hasher_test.go
)

IF (ARCH_X86_64)
    SRCS(
        accum_stubs_amd64.go
        accum_vector_avx512_amd64.s
        accum_vector_avx_amd64.s
        accum_vector_sse_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(accum_stubs_other.go)
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(accum_stubs_other.go)
ENDIF()

END()

RECURSE(gotest)
