GO_LIBRARY()

LICENSE(MIT)

SRCS(
    area.go
    contains.go
    distance.go
    distance_from.go
    length.go
)

GO_TEST_SRCS(
    area_test.go
    contains_test.go
    distance_from_test.go
    distance_test.go
    length_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
