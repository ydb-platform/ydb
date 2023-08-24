GO_LIBRARY()

SRCS(
    arrow_helpers.go
    columnar_buffer_arrow_ipc_streaming.go
    columnar_buffer_factory.go
    converters.go
    errors.go
    endpoint.go
    logger.go
    paging_writer.go
    protobuf.go
    read_limiter.go
    select_helpers.go
    time.go
    type_mapper.go
)

GO_TEST_SRCS(
    time_test.go
)

END()
