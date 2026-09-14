GTEST(unittester-core-yaml)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    convert_ut.cpp
    parser_ut.cpp
    writer_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/core/yaml
)

END()
