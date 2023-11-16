GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    baggage.go
    context.go
    doc.go
)

GO_TEST_SRCS(
    baggage_test.go
    context_test.go
)

END()

RECURSE(gotest)
