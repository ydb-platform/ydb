GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(registry.go)

GO_XTEST_SRCS(registry_test.go)

END()

RECURSE(gotest)
