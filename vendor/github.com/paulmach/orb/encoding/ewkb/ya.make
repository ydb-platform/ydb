GO_LIBRARY()

LICENSE(MIT)

SRCS(
    ewkb.go
    scanner.go
)

GO_TEST_SRCS(
    collection_test.go
    ewkb_test.go
    line_string_test.go
    point_test.go
    polygon_test.go
    scanner_test.go
)

END()

RECURSE(gotest)
