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
    base_arithmetic.go
    boolean_cast.go
    cast.go
    cast_numeric.go
    cast_temporal.go
    compareoperator_string.go
    constant_factor.go
    doc.go
    helpers.go
    numeric_cast.go
    rounding.go
    roundmode_string.go
    scalar_arithmetic.go
    scalar_boolean.go
    scalar_comparisons.go
    string_casts.go
    types.go
    vector_hash.go
    vector_run_end_encode.go
    vector_selection.go
)

IF (ARCH_X86_64)
    SRCS(
        base_arithmetic_amd64.go
        base_arithmetic_avx2_amd64.go
        base_arithmetic_avx2_amd64.s
        base_arithmetic_sse4_amd64.go
        base_arithmetic_sse4_amd64.s
        cast_numeric_amd64.go
        cast_numeric_avx2_amd64.go
        cast_numeric_avx2_amd64.s
        cast_numeric_sse4_amd64.go
        cast_numeric_sse4_amd64.s
        constant_factor_amd64.go
        constant_factor_avx2_amd64.go
        constant_factor_avx2_amd64.s
        constant_factor_sse4_amd64.go
        constant_factor_sse4_amd64.s
        scalar_comparison_amd64.go
        scalar_comparison_avx2_amd64.go
        scalar_comparison_avx2_amd64.s
        scalar_comparison_sse4_amd64.go
        scalar_comparison_sse4_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        basic_arithmetic_noasm.go
        cast_numeric_neon_arm64.go
        cast_numeric_neon_arm64.s
        scalar_comparison_noasm.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        basic_arithmetic_noasm.go
        cast_numeric_neon_arm64.go
        cast_numeric_neon_arm64.s
        scalar_comparison_noasm.go
    )
ENDIF()

END()
