GO_LIBRARY()

SRCS(
    utf16.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(utf16_test.go)

END()

RECURSE(
)
