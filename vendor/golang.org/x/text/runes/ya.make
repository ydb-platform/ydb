GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    cond.go
    runes.go
)

GO_TEST_SRCS(
    cond_test.go
    runes_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
