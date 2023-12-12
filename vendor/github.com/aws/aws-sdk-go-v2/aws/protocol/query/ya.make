GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    array.go
    encoder.go
    map.go
    middleware.go
    object.go
    value.go
)

GO_TEST_SRCS(
    encoder_test.go
    middleware_test.go
)

END()

RECURSE(
    gotest
)
