GO_LIBRARY()

LICENSE(MIT)

SRCS(rdv.go)

GO_TEST_SRCS(rdv_test.go)

END()

RECURSE(gotest)
