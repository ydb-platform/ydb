GO_LIBRARY()

SRCS(
    chunk.go
    contains.go
    contains_all.go
    contains_any.go
    dedup.go
    equal.go
    filter.go
    group_by.go
    intersects.go
    join.go
    map.go
    reverse.go
    shuffle.go
)

GO_XTEST_SRCS(
    chunk_test.go
    dedup_test.go
    equal_test.go
    filter_test.go
    group_by_test.go
    intersects_test.go
    join_test.go
    map_test.go
    reverse_test.go
    shuffle_test.go
)

END()

RECURSE(gotest)
