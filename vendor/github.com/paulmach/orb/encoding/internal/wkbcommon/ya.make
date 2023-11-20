GO_LIBRARY()

LICENSE(MIT)

SRCS(
    collection.go
    line_string.go
    point.go
    polygon.go
    scan.go
    wkb.go
)

GO_TEST_SRCS(
    collection_test.go
    line_string_test.go
    point_test.go
    polygon_test.go
    scan_test.go
    wkb_test.go
)

END()

RECURSE(gotest)
