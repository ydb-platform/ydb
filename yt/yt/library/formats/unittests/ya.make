GTEST(unittester-formats)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    protobuf_format_ut.proto

    arrow_parser_ut.cpp
    dsv_parser_ut.cpp
    dsv_writer_ut.cpp
    protobuf_format_ut.cpp
    row_helpers.cpp
    schemaful_dsv_parser_ut.cpp
    schemaful_dsv_writer_ut.cpp
    skiff_format_ut.cpp
    skiff_yson_converter_ut.cpp
    value_examples.cpp
    web_json_writer_ut.cpp
    yamred_dsv_parser_ut.cpp
    yamred_dsv_writer_ut.cpp
    yaml_parser_ut.cpp
    yaml_writer_ut.cpp
    yamr_parser_ut.cpp
    yamr_writer_ut.cpp
    yson_helpers.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/core
    yt/yt/client
    yt/yt/client/formats
    yt/yt/library/formats
    yt/yt/library/named_value

    contrib/libs/apache/arrow
)

RESOURCE(
    ${ARCADIA_ROOT}/library/cpp/type_info/ut/test-data/good-types.txt /types/good
    ${ARCADIA_ROOT}/library/cpp/type_info/ut/test-data/bad-types.txt /types/bad
)

SIZE(MEDIUM)

REQUIREMENTS(ram:12)

END()
