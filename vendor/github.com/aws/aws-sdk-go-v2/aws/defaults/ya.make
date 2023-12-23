GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    auto.go
    configuration.go
    defaults.go
    doc.go
)

GO_TEST_SRCS(
    auto_test.go
    defaults_codegen_test.go
    defaults_test.go
)

END()

RECURSE(
    gotest
)
