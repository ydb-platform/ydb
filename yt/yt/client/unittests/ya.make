GTEST(unittester-client)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

PROTO_NAMESPACE(yt)

SRCS(
    protobuf_format_ut.proto

    check_schema_compatibility_ut.cpp
    check_type_compatibility_ut.cpp
    chunk_replica_ut.cpp
    column_sort_schema_ut.cpp
    comparator_ut.cpp
    composite_compare_ut.cpp
    connection_ut.cpp
    dsv_parser_ut.cpp
    dsv_writer_ut.cpp
    farm_fingerprint_stability_ut.cpp
    key_bound_ut.cpp
    key_bound_compressor_ut.cpp
    key_helpers.cpp
    key_ut.cpp
    logical_type_ut.cpp
    named_yson_token_ut.cpp
    uuid_text_ut.cpp
    time_text_ut.cpp
    node_directory_ut.cpp
    protobuf_format_ut.cpp
    query_builder_ut.cpp
    read_limit_ut.cpp
    replication_progress_ut.cpp
    row_helpers.cpp
    row_ut.cpp
    schemaful_dsv_parser_ut.cpp
    schemaful_dsv_writer_ut.cpp
    schema_ut.cpp
    skiff_format_ut.cpp
    skiff_yson_converter_ut.cpp
    table_consumer_ut.cpp
    unordered_reader_ut.cpp
    unversioned_row_ut.cpp
    validate_logical_type_ut.cpp
    value_examples.cpp
    web_json_writer_ut.cpp
    wire_protocol_ut.cpp
    yamred_dsv_parser_ut.cpp
    yamred_dsv_writer_ut.cpp
    yamr_parser_ut.cpp
    yamr_writer_ut.cpp
    ypath_ut.cpp
    yson_helpers.cpp
    zookeeper_bus_ut.cpp
    zookeeper_protocol_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/core
    yt/yt/client
    yt/yt/client/formats
    yt/yt/client/unittests/mock
    yt/yt/library/named_value

    yt/yt_proto/yt/formats
)

RESOURCE(
    ${ARCADIA_ROOT}/library/cpp/type_info/ut/test-data/good-types.txt /types/good
    ${ARCADIA_ROOT}/library/cpp/type_info/ut/test-data/bad-types.txt /types/bad
)

SIZE(MEDIUM)

REQUIREMENTS(ram:12)

END()
