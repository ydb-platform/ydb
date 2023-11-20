GO_LIBRARY()

SRCS(
    chunked.go
)

GO_TEST_SRCS(chunked_test.go)

END()

RECURSE(
    ascii
    testcert
)
