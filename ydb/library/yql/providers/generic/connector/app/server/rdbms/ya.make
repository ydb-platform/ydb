GO_LIBRARY()

SRCS(
    handler.go
    handler_factory.go
    schema_builder.go
)

GO_TEST_SRCS(
    schema_builder_test.go
)

END()

RECURSE_FOR_TESTS(ut)