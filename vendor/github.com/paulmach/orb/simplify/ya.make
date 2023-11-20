GO_LIBRARY()

LICENSE(MIT)

SRCS(
    douglas_peucker.go
    helpers.go
    radial.go
    visvalingam.go
)

GO_TEST_SRCS(
    benchmarks_test.go
    douglas_peucker_test.go
    helpers_test.go
    radial_test.go
    visvalingam_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
