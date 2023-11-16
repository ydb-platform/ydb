GO_LIBRARY()

SRCS(
    formdata.go
    multipart.go
    readmimeheader.go
    writer.go
)

GO_TEST_SRCS(
    formdata_test.go
    multipart_test.go
    writer_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
