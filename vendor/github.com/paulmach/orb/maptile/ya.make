GO_LIBRARY()

LICENSE(MIT)

SRCS(
    set.go
    tile.go
)

GO_TEST_SRCS(tile_test.go)

END()

RECURSE(
    gotest
    tilecover
)
