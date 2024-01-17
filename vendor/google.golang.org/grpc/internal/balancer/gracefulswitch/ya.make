GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(gracefulswitch.go)

GO_TEST_SRCS(gracefulswitch_test.go)

END()

RECURSE(gotest)
