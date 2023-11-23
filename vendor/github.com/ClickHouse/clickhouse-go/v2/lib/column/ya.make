GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    array.go
    bigint.go
    bool.go
    column.go
    column_gen.go
    date.go
    date32.go
    date_helpers.go
    datetime.go
    datetime64.go
    decimal.go
    enum.go
    enum16.go
    enum8.go
    fixed_string.go
    geo_multi_polygon.go
    geo_point.go
    geo_polygon.go
    geo_ring.go
    interval.go
    ipv4.go
    ipv6.go
    json.go
    lowcardinality.go
    map.go
    nested.go
    nothing.go
    nullable.go
    simple_aggregate_function.go
    slice_helper.go
    string.go
    tuple.go
    uuid.go
)

END()

RECURSE(codegen)
