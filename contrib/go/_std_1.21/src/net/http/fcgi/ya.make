GO_LIBRARY()

SRCS(
    child.go
    fcgi.go
)

GO_TEST_SRCS(fcgi_test.go)

END()

RECURSE(
)
