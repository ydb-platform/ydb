GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(hashtag.go)

GO_TEST_SRCS(hashtag_test.go)

END()

RECURSE(gotest)
