GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    appengine.go
    appengine_gen2_flex.go
    default.go
    doc.go
    error.go
    google.go
    jwt.go
    sdk.go
)

GO_TEST_SRCS(
    default_test.go
    error_test.go
    google_test.go
    jwt_test.go
    sdk_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    downscope
    gotest
    internal
)
