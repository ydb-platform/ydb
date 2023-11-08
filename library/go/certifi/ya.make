GO_LIBRARY()

SRCS(
    cas.go
    certifi.go
    doc.go
    utils.go
)

GO_XTEST_SRCS(
    certifi_example_test.go
    certifi_test.go
    utils_test.go
)

END()

RECURSE(
    gotest
    internal
)
