GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    docopt.go
    error.go
    opts.go
    pattern.go
    token.go
)

GO_TEST_SRCS(
    docopt_test.go
    example_test.go
    opts_test.go
)

END()

RECURSE(gotest)
