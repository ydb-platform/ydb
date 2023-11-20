GO_LIBRARY()

LICENSE(MIT)

SRCS(
    clip.go
    helpers.go
    options.go
)

GO_TEST_SRCS(
    clip_test.go
    helpers_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    smartclip
)
