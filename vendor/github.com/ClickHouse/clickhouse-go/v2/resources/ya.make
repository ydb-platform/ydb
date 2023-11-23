GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(meta.go)

GO_TEST_SRCS(meta_test.go)

GO_EMBED_PATTERN(meta.yml)

END()
