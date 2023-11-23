GO_LIBRARY()

SRCS(
    color.go
    ycbcr.go
)

GO_TEST_SRCS(
    color_test.go
    ycbcr_test.go
)

END()

RECURSE(
    palette
)
