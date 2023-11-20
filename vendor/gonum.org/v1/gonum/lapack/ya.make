GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
    lapack.go
)

END()

RECURSE(
    gonum
    lapack64
    testlapack
)
