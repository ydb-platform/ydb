GO_LIBRARY()

SRCS(
    utf8.go
)

GO_XTEST_SRCS(
    example_test.go
    utf8_test.go
)

END()

RECURSE(
)
