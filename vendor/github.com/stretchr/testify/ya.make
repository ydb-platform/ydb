GO_LIBRARY()

OWNER(g:go-contrib)

LICENSE(MIT)

SRCS(doc.go)

GO_TEST_SRCS(package_test.go)

END()

RECURSE(
    assert
    gotest
    http
    mock
    require
    #suite
)
