GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.5.1)

SRCS(
    google_api_http.go
)

GO_XTEST_SRCS(google_api_http_test.go)

END()

RECURSE(
    gotest
)
