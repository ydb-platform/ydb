GO_LIBRARY()

SRCS(
    format.go
    geom.go
    image.go
    names.go
    ycbcr.go
)

GO_TEST_SRCS(
    geom_test.go
    image_test.go
    ycbcr_test.go
)

GO_XTEST_SRCS(
    decode_example_test.go
    decode_test.go
)

END()

RECURSE(
    color
    draw
    gif
    internal
    jpeg
    png
)
