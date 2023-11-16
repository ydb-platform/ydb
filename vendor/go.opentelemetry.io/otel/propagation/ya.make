GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    baggage.go
    doc.go
    propagation.go
    trace_context.go
)

GO_XTEST_SRCS(
    baggage_test.go
    propagation_test.go
    propagators_test.go
    trace_context_benchmark_test.go
    trace_context_example_test.go
    trace_context_test.go
)

END()

RECURSE(gotest)
