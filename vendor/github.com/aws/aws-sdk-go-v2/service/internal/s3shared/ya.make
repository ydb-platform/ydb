GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    arn_lookup.go
    endpoint_error.go
    go_module_metadata.go
    host_id.go
    metadata.go
    metadata_retriever.go
    resource_request.go
    response_error.go
    response_error_middleware.go
    s3100continue.go
    update_endpoint.go
    xml_utils.go
)

GO_TEST_SRCS(
    s3100continue_test.go
    xml_utils_test.go
)

END()

RECURSE(
    arn
    config
    gotest
)
