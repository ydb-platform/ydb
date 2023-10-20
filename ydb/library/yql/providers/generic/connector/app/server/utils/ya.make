GO_LIBRARY()

SRCS(
    arrow_helpers.go
    connection_manager.go
    converters.go
    doc.go
    endpoint.go
    errors.go
    logger.go
    protobuf.go
    query_builder.go
    select_helpers.go
    time.go
    type_mapper.go
)

GO_TEST_SRCS(
    time_test.go
)

END()
