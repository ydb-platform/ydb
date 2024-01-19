LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    arrow_parser.cpp
    arrow_writer.cpp
    dsv_parser.cpp
    dsv_writer.cpp
    escape.cpp
    format.cpp
    helpers.cpp
    protobuf.cpp
    protobuf_options.cpp
    protobuf_parser.cpp
    protobuf_writer.cpp
    schemaful_dsv_parser.cpp
    schemaful_dsv_writer.cpp
    schemaless_writer_adapter.cpp
    skiff_parser.cpp
    skiff_writer.cpp
    skiff_yson_converter.cpp
    unversioned_value_yson_writer.cpp
    web_json_writer.cpp
    yamred_dsv_parser.cpp
    yamred_dsv_writer.cpp
    yamr_parser_base.cpp
    yamr_parser.cpp
    yamr_writer_base.cpp
    yamr_writer.cpp
    yql_yson_converter.cpp
    yson_map_to_unversioned_value.cpp
    yson_parser.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/client/formats
    yt/yt/client/arrow/fbs
    yt/yt/library/column_converters

    contrib/libs/apache/arrow
)

END()

RECURSE_FOR_TESTS(
    unittests
)

IF (NOT OPENSOURCE)
    RECURSE(
        benchmark
    )
ENDIF()
