GO_LIBRARY()

LICENSE(MIT)

SRCS(
    around_bound.go
    smart.go
)

GO_TEST_SRCS(
    around_bound_test.go
    smart_test.go
    util_test.go
)

END()

RECURSE(gotest)
