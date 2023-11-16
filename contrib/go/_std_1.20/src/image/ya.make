GO_LIBRARY()

SRCS(
    format.go
    geom.go
    image.go
    names.go
    ycbcr.go
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
