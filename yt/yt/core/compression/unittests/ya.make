GTEST(unittester-core-compression)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    codec_ut.cpp
    dictionary_compression_ut.cpp
    stream_ut.cpp
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

SIZE(MEDIUM)

IF (OS_DARWIN)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:exotic_platform)
ENDIF()

END()
