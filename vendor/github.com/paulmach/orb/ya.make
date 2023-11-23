GO_LIBRARY()

LICENSE(MIT)

SRCS(
    bound.go
    clone.go
    define.go
    equal.go
    geometry.go
    line_string.go
    multi_line_string.go
    multi_point.go
    multi_polygon.go
    point.go
    polygon.go
    ring.go
    round.go
)

GO_TEST_SRCS(
    bound_test.go
    clone_test.go
    equal_test.go
    geometry_test.go
    json_test.go
    line_string_test.go
    multi_line_string_test.go
    multi_point_test.go
    multi_polygon_test.go
    point_test.go
    ring_test.go
    round_test.go
)

END()

RECURSE(
    clip
    encoding
    geo
    geojson
    gotest
    internal
    maptile
    planar
    project
    quadtree
    resample
    simplify
)
