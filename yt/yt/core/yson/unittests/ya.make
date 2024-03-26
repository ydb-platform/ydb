GTEST(unittester-core-yson)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (NOT OS_WINDOWS AND NOT ARCH_AARCH64)
    ALLOCATOR(YT)
ENDIF()

PROTO_NAMESPACE(yt)

SRCS(
    depth_limiting_yson_consumer_ut.cpp
    filter_ut.cpp
    lexer_ut.cpp
    list_verb_lazy_yson_consumer_ut.cpp
    protobuf_scalar_type_ut.cpp
    protobuf_yson_schema_ut.cpp
    protobuf_yson_ut.cpp
    ypath_designated_yson_consumer_ut.cpp
    ypath_filtering_yson_consumer_ut.cpp
    yson_parser_ut.cpp
    yson_pull_parser_ut.cpp
    yson_token_writer_ut.cpp
    yson_ut.cpp
    yson_writer_ut.cpp

    proto/protobuf_scalar_type_ut.proto
    proto/protobuf_yson_ut.proto
    proto/protobuf_yson_casing_ut.proto
    proto/protobuf_yson_casing_ext_ut.proto
    proto/protobuf_yson_schema_ut.proto
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework
)

REQUIREMENTS(
    cpu:4
    ram:4
    ram_disk:1
)

FORK_TESTS()

SPLIT_FACTOR(5)

SIZE(SMALL)

IF (OS_DARWIN)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:exotic_platform)
ENDIF()

ENV(ASAN_OPTIONS="detect_leaks=0")

END()
