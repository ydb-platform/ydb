GO_LIBRARY()

SRCS(
    entity.go
    escape.go
)

GO_TEST_SRCS(
    entity_test.go
    escape_test.go
    fuzz_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    template
)
