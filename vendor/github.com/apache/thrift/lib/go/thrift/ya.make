GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause
)

SRCS(
    application_exception.go
    binary_protocol.go
    buffered_transport.go
    client.go
    compact_protocol.go
    configuration.go
    context.go
    debug_protocol.go
    deserializer.go
    duplicate_protocol.go
    exception.go
    framed_transport.go
    header_context.go
    header_protocol.go
    header_transport.go
    http_client.go
    http_transport.go
    iostream_transport.go
    json_protocol.go
    logger.go
    memory_buffer.go
    messagetype.go
    middleware.go
    multiplexed_protocol.go
    numeric.go
    pointerize.go
    pool.go
    processor_factory.go
    protocol.go
    protocol_exception.go
    protocol_factory.go
    response_helper.go
    rich_transport.go
    serializer.go
    server.go
    server_socket.go
    server_transport.go
    simple_json_protocol.go
    simple_server.go
    socket.go
    socket_conn.go
    ssl_server_socket.go
    ssl_socket.go
    transport.go
    transport_exception.go
    transport_factory.go
    type.go
    uuid.go
    zlib_transport.go
)

GO_TEST_SRCS(
    application_exception_test.go
    binary_protocol_test.go
    buffered_transport_test.go
    common_test.go
    compact_protocol_test.go
    configuration_test.go
    example_client_middleware_test.go
    example_processor_middleware_test.go
    exception_test.go
    framed_transport_test.go
    header_context_test.go
    header_protocol_test.go
    header_transport_test.go
    http_client_test.go
    iostream_transport_test.go
    json_protocol_test.go
    lowlevel_benchmarks_test.go
    memory_buffer_test.go
    middleware_test.go
    multiplexed_protocol_test.go
    pool_test.go
    protocol_test.go
    response_helper_test.go
    rich_transport_test.go
    serializer_test.go
    serializer_types_test.go
    server_socket_test.go
    server_test.go
    simple_json_protocol_test.go
    simple_server_test.go
    socket_conn_test.go
    transport_exception_test.go
    transport_test.go
    uuid_test.go
    zlib_transport_test.go
)

IF (OS_LINUX)
    SRCS(socket_unix_conn.go)

    GO_TEST_SRCS(socket_unix_conn_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(socket_unix_conn.go)

    GO_TEST_SRCS(socket_unix_conn_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(socket_non_unix_conn.go)
ENDIF()

END()

RECURSE(gotest)
