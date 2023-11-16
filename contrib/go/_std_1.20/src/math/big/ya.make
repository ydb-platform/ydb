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

IF (ARCH_ARM64)
    SRCS(
        arith_arm64.s
    )
ENDIF()

IF (ARCH_X86_64)
    SRCS(
        arith_amd64.go
        arith_amd64.s
    )
ENDIF()

END()
