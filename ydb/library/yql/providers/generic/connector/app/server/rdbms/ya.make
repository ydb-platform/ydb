GO_LIBRARY()

SRCS(
    doc.go
    handler.go
    handler_factory.go
    interface.go
    mock.go
    predicate_builder.go
    schema_builder.go
)

GO_TEST_SRCS(
    schema_builder_test.go
)

END()

RECURSE_FOR_TESTS(ut)
