GO_LIBRARY()

LICENSE(MIT)

SRCS(pgpass.go)

GO_TEST_SRCS(pgpass_test.go)

END()

RECURSE(gotest)
