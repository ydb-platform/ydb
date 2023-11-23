GO_LIBRARY()

SRCS(
    ioutil.go
    tempfile.go
)

GO_XTEST_SRCS(
    example_test.go
    ioutil_test.go
    tempfile_test.go
)

END()

RECURSE(
)
