LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    dsv_parser.cpp
    dsv_writer.cpp
    escape.cpp
    format.cpp
    helpers.cpp
    parser.cpp
    protobuf.cpp
    protobuf_options.cpp
    protobuf_parser.cpp
    protobuf_writer.cpp
    schemaful_dsv_parser.cpp
    schemaful_dsv_writer.cpp
    schemaful_writer.cpp
    schemaless_writer_adapter.cpp
    web_json_writer.cpp
    skiff_parser.cpp
    skiff_yson_converter.cpp
    skiff_writer.cpp
    unversioned_value_yson_writer.cpp
    versioned_writer.cpp
    yamr_parser.cpp
    yamr_parser_base.cpp
    yamr_writer.cpp
    yamr_writer_base.cpp
    yamred_dsv_parser.cpp
    yamred_dsv_writer.cpp
    yson_parser.cpp
    yson_map_to_unversioned_value.cpp
    yql_yson_converter.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/library/skiff_ext
    yt/yt_proto/yt/formats
    library/cpp/string_utils/base64
)

END()
