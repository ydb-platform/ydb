GO_LIBRARY()

LICENSE(MIT)

SRCS(pgservicefile.go)

GO_XTEST_SRCS(pgservicefile_test.go)

END()

RECURSE(gotest)
