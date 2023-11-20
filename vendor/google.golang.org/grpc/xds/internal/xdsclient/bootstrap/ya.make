GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    bootstrap.go
    logging.go
    template.go
)

GO_TEST_SRCS(
    bootstrap_test.go
    template_test.go
)

END()

RECURSE(gotest)
