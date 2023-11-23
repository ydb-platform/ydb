GO_LIBRARY()

LICENSE(MIT)

SRCS(
    date.go
    uuid.go
)

GO_TEST_SRCS(uuid_test.go)

END()

RECURSE(gotest)
