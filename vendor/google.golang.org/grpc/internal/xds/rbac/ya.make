GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    converter.go
    matchers.go
    rbac_engine.go
)

GO_TEST_SRCS(
    converter_test.go
    rbac_engine_test.go
)

END()

RECURSE(gotest)
