GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    stacks.go
)

GO_TEST_SRCS(stacks_test.go)

END()

RECURSE(gotest)
