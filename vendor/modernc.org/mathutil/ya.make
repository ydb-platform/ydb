GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    binarylog.go
    bits.go
    envelope.go
    int.go
    mathutil.go
    permute.go
    poly.go
    primes.go
    rat.go
    rnd.go
    sqr.go
    tables.go
    test_deps.go
)

GO_TEST_SRCS(
    all_test.go
    binarylog_test.go
    int_test.go
)

END()

RECURSE(gotest)
