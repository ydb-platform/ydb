GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(doc.go)

IF (ARCH_X86_64)
    SRCS(
        abssum_amd64.s
        abssuminc_amd64.s
        add_amd64.s
        addconst_amd64.s
        axpyinc_amd64.s
        axpyincto_amd64.s
        axpyunitary_amd64.s
        axpyunitaryto_amd64.s
        cumprod_amd64.s
        cumsum_amd64.s
        div_amd64.s
        divto_amd64.s
        dot_amd64.s
        ge_amd64.go
        gemvN_amd64.s
        gemvT_amd64.s
        ger_amd64.s
        l1norm_amd64.s
        l2norm_amd64.s
        l2normdist_amd64.s
        l2norminc_amd64.s
        linfnorm_amd64.s
        scalinc_amd64.s
        scalincto_amd64.s
        scalunitary_amd64.s
        scalunitaryto_amd64.s
        stubs_amd64.go
        sum_amd64.s
    )
ENDIF()

IF (ARCH_ARM64)
    SRCS(
        axpy.go
        dot.go
        ge_noasm.go
        l2norm_noasm.go
        scal.go
        stubs_noasm.go
    )
ENDIF()

END()
