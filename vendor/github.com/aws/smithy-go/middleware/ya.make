GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    logging.go
    metadata.go
    middleware.go
    ordered_group.go
    stack.go
    stack_values.go
    step_build.go
    step_deserialize.go
    step_finalize.go
    step_initialize.go
    step_serialize.go
)

GO_TEST_SRCS(
    metadata_test.go
    middleware_test.go
    ordered_group_test.go
    shared_test.go
    stack_test.go
    stack_values_test.go
)

GO_XTEST_SRCS(logging_test.go)

END()

RECURSE(
    gotest
)
