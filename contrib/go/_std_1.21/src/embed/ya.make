GO_LIBRARY()

SRCS(
    embed.go
)

GO_XTEST_SRCS(example_test.go)

GO_XTEST_EMBED_PATTERN(internal/embedtest/testdata/*.txt)

END()

RECURSE(
    # internal # tests don't work due to fail to embed hidden folders
)
