GO_LIBRARY()

SRCS(
    io.go
    multi.go
    pipe.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    example_test.go
    io_test.go
    multi_test.go
    pipe_test.go
)

END()

RECURSE(
    fs
    ioutil
)
