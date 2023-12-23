GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    checksum_middleware.go
    client.go
    doc.go
    headerlist.go
    host.go
    md5_checksum.go
    middleware_close_response_body.go
    middleware_content_length.go
    middleware_header_comment.go
    middleware_headers.go
    middleware_http_logging.go
    middleware_metadata.go
    middleware_min_proto.go
    request.go
    response.go
    time.go
    url.go
    user_agent.go
)

GO_TEST_SRCS(
    checksum_middleware_test.go
    client_test.go
    deserialize_example_test.go
    headerlist_test.go
    host_test.go
    middleware_content_length_test.go
    middleware_header_comment_test.go
    request_test.go
    response_error_example_test.go
    serialize_example_test.go
    url_test.go
    user_agent_test.go
)

GO_XTEST_SRCS(
    middleware_headers_test.go
    middleware_http_logging_test.go
)

END()

RECURSE(
    gotest
    internal
)
