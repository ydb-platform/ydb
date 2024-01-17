GO_LIBRARY()

SRCS(
    abs.go
    asin.go
    conj.go
    exp.go
    isinf.go
    isnan.go
    log.go
    phase.go
    polar.go
    pow.go
    rect.go
    sin.go
    sqrt.go
    tan.go
)

GO_TEST_SRCS(
    cmath_test.go
    huge_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
