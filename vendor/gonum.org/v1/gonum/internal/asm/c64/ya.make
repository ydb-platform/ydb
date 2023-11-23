GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    conj.go
    doc.go
    scal.go
    stubs.go
)

IF (ARCH_X86_64)
    SRCS(
        axpyinc_amd64.s
        axpyincto_amd64.s
        axpyunitary_amd64.s
        axpyunitaryto_amd64.s
        dotcinc_amd64.s
        dotcunitary_amd64.s
        dotuinc_amd64.s
        dotuunitary_amd64.s
        stubs_amd64.go
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(stubs_noasm.go)
ENDIF()

END()
