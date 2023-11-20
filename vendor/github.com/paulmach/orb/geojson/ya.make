GO_LIBRARY()

LICENSE(MIT)

SRCS(
    bbox.go
    feature.go
    feature_collection.go
    geometry.go
    json.go
    properties.go
    types.go
)

GO_TEST_SRCS(
    bbox_test.go
    feature_collection_test.go
    feature_test.go
    geometry_test.go
    properties_test.go
)

GO_XTEST_SRCS(
    example_pointer_test.go
    example_test.go
)

END()

RECURSE(gotest)
