GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    sort.go
    xmlToStruct.go
)

GO_TEST_SRCS(sort_test.go)

END()

RECURSE(
    gotest
)
