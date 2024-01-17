GO_LIBRARY()

SRCS(
    accuracy_string.go
    arith.go
    arith_decl.go
    decimal.go
    doc.go
    float.go
    floatconv.go
    floatmarsh.go
    ftoa.go
    int.go
    intconv.go
    intmarsh.go
    nat.go
    natconv.go
    natdiv.go
    prime.go
    rat.go
    ratconv.go
    ratmarsh.go
    roundingmode_string.go
    sqrt.go
)

GO_TEST_SRCS(
    arith_test.go
    bits_test.go
    calibrate_test.go
    decimal_test.go
    float_test.go
    floatconv_test.go
    floatmarsh_test.go
    gcd_test.go
    hilbert_test.go
    int_test.go
    intconv_test.go
    intmarsh_test.go
    link_test.go
    nat_test.go
    natconv_test.go
    prime_test.go
    rat_test.go
    ratconv_test.go
    ratmarsh_test.go
    sqrt_test.go
)

GO_XTEST_SRCS(
    alias_test.go
    example_rat_test.go
    example_test.go
    floatexample_test.go
)

IF (ARCH_X86_64)
    SRCS(
        arith_amd64.go
        arith_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        arith_arm64.s
    )
ENDIF()

END()

RECURSE(
)
