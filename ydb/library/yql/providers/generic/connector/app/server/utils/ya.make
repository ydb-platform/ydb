GO_LIBRARY()

SRCS(
    arrow_helpers.go
    counter.go
    converters.go
    doc.go
    endpoint.go
    errors.go
    logger.go
    predicate_builder.go
    protobuf.go
    select_helpers.go
    sql.go
    sql_mock.go
    time.go
    type_mapper.go
    unit_test_helpers.go
)

GO_TEST_SRCS(
    counter_test.go
    time_test.go
    unit_test_helpers_test.go
)

END()

RECURSE_FOR_TESTS(
    ut
)
