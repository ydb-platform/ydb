GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    callback_serializer.go
    event.go
    oncefunc.go
)

GO_TEST_SRCS(
    callback_serializer_test.go
    event_test.go
    oncefunc_test.go
)

END()

RECURSE(gotest)
