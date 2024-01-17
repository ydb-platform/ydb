GO_LIBRARY()

SRCS(
    exp.go
    normal.go
    rand.go
    rng.go
    zipf.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    auto_test.go
    default_test.go
    example_test.go
    race_test.go
    rand_test.go
    regress_test.go
)

END()

RECURSE(
)
