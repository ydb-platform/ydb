GO_LIBRARY()

SRCS(
    annotation.go
    trace.go
)

GO_XTEST_SRCS(
    annotation_test.go
    example_test.go
    trace_stack_test.go
    trace_test.go
)

END()

RECURSE(
)
