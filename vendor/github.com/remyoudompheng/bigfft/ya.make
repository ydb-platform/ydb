GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    arith_decl.go
    fermat.go
    fft.go
    scan.go
)

GO_TEST_SRCS(
    calibrate_test.go
    fermat_test.go
    fft_test.go
    scan_test.go
)

END()

RECURSE(gotest)
