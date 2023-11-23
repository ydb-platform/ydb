GO_LIBRARY()

LICENSE(MIT)

SRCS(mercator.go)

GO_TEST_SRCS(mercator_test.go)

END()

RECURSE(gotest)
