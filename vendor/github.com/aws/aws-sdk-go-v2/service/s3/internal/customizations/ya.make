GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    handle_200_error.go
    host.go
    presigned_expires.go
    process_arn_resource.go
    remove_bucket_middleware.go
    s3_object_lambda.go
    signer_wrapper.go
    update_endpoint.go
)

GO_TEST_SRCS(
    # update_endpoint_internal_test.go
)

GO_XTEST_SRCS(
    handle_200_error_test.go
    presign_test.go
    unit_test.go
    update_endpoint_test.go
    write_get_object_response_test.go
)

END()

RECURSE(
    gotest
)
