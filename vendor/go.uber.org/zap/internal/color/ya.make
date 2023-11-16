GO_LIBRARY()

LICENSE(MIT)

SRCS(
    color.go
)

GO_TEST_SRCS(color_test.go)

END()

RECURSE(
    gotest
)
