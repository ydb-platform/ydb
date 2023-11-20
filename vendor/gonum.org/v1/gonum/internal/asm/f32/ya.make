GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    gemv.go
    l2norm.go
    scal.go
)

IF (ARCH_X86_64)
    SRCS(
        axpyinc_amd64.s
        axpyincto_amd64.s
        axpyunitary_amd64.s
        axpyunitaryto_amd64.s
        ddotinc_amd64.s
        ddotunitary_amd64.s
        dotinc_amd64.s
        dotunitary_amd64.s
        ge_amd64.go
        ge_amd64.s
        stubs_amd64.go
        sum_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        ge_noasm.go
        stubs_noasm.go
    )
ENDIF()

END()
