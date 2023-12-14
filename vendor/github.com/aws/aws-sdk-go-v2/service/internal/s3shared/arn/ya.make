GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    accesspoint_arn.go
    arn.go
    arn_member.go
    outpost_arn.go
    s3_object_lambda_arn.go
)

GO_TEST_SRCS(
    accesspoint_arn_test.go
    arn_test.go
    outpost_arn_test.go
)

END()

RECURSE(
    gotest
)
