GO_LIBRARY()

SRCS(
    client.go
    server.go
)

GO_TEST_SRCS(all_test.go)

END()

RECURSE(
)
