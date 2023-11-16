GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    exp.go
    normal.go
    rand.go
    rng.go
    zipf.go
)

GO_TEST_SRCS(
    arith128_test.go
    modulo_test.go
    race_test.go
    rand_test.go
)

GO_XTEST_SRCS(
    example_test.go
    regress_test.go
)

END()

RECURSE(gotest)
