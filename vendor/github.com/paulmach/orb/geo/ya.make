GO_LIBRARY()

LICENSE(MIT)

SRCS(
    area.go
    bound.go
    distance.go
    length.go
)

GO_TEST_SRCS(
    area_test.go
    bound_test.go
    distance_test.go
    length_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
