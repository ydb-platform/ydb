GO_LIBRARY()

SRCS(
    httptest.go
    recorder.go
    server.go
)

GO_TEST_SRCS(
    httptest_test.go
    recorder_test.go
    server_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
