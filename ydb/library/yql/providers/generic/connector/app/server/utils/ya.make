GO_LIBRARY()

SRCS(
    arrow_helpers.go
    columnar_buffer_arrow_ipc_streaming.go
    columnar_buffer_factory.go
    connection_manager.go
    converters.go
    endpoint.go
    errors.go
    logger.go
    paging_writer.go
    protobuf.go
    query_builder.go
    read_limiter.go
    select_helpers.go
    time.go
    type_mapper.go
)

GO_TEST_SRCS(
    time_test.go
)

END()
