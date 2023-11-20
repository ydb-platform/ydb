GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    config.go
    context.go
    doc.go
    nonrecording.go
    noop.go
    trace.go
    tracestate.go
)

GO_TEST_SRCS(
    config_test.go
    context_test.go
    noop_test.go
    trace_test.go
    tracestate_test.go
)

END()

RECURSE(gotest)
