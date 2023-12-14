GO_LIBRARY()

SRCS(
    arrow_helpers.go
    converters.go
    counter.go
    doc.go
    endpoint.go
    errors.go
    logger.go
    protobuf.go
    row_transformer.go
    select_helpers.go
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
