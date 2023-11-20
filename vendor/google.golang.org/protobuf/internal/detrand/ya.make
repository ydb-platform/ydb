GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(rand.go)

GO_TEST_SRCS(rand_test.go)

END()

RECURSE(gotest)
