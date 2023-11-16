GO_LIBRARY()

LICENSE(MIT)

SRCS(
    array.go
    array_codec.go
    bits.go
    bool.go
    box.go
    builtin_wrappers.go
    bytea.go
    circle.go
    composite.go
    convert.go
    date.go
    doc.go
    enum_codec.go
    float4.go
    float8.go
    hstore.go
    inet.go
    int.go
    interval.go
    json.go
    jsonb.go
    line.go
    lseg.go
    macaddr.go
    multirange.go
    numeric.go
    path.go
    pgtype.go
    point.go
    polygon.go
    qchar.go
    range.go
    range_codec.go
    record_codec.go
    register_default_pg_types.go
    text.go
    text_format_only_codec.go
    tid.go
    time.go
    timestamp.go
    timestamptz.go
    uint32.go
    uuid.go
)

GO_TEST_SRCS(
    array_test.go
    range_test.go
)

GO_XTEST_SRCS(
    # array_codec_test.go
    # bits_test.go
    # bool_test.go
    # box_test.go
    # bytea_test.go
    # circle_test.go
    # composite_test.go # st/YMAKE-102
    # date_test.go
    # enum_codec_test.go # st/YMAKE-102
    # example_child_records_test.go # st/YMAKE-102
    # example_custom_type_test.go # st/YMAKE-102
    # example_json_test.go # st/YMAKE-102
    # float4_test.go
    # float8_test.go
    # hstore_test.go
    # inet_test.go
    # int_test.go
    # integration_benchmark_test.go # st/YMAKE-102
    # interval_test.go
    # json_test.go
    # jsonb_test.go
    # line_test.go
    # lseg_test.go
    # macaddr_test.go
    # multirange_test.go
    # numeric_test.go
    # path_test.go
    # pgtype_test.go # st/YMAKE-102
    # point_test.go
    # polygon_test.go
    # qchar_test.go
    # range_codec_test.go
    # record_codec_test.go # st/YMAKE-102
    # text_test.go
    # tid_test.go
    # time_test.go
    # timestamp_test.go
    # timestamptz_test.go
    # uint32_test.go
    # uuid_test.go
)

END()

RECURSE(
    gotest
    zeronull
)
