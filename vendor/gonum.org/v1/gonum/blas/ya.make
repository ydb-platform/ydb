GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    blas.go
    doc.go
)

END()

RECURSE(
    blas32
    blas64
    cblas128
    cblas64
    gonum
    testblas
)
