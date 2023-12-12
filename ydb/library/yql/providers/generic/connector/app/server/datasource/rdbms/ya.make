GO_LIBRARY()

SRCS(
    data_source.go
    data_source_factory.go
    doc.go
    schema_builder.go
)

GO_TEST_SRCS(
    data_source_test.go
    schema_builder_test.go
)

END()

RECURSE(
    clickhouse
    postgresql
    utils
)

RECURSE_FOR_TESTS(ut)
