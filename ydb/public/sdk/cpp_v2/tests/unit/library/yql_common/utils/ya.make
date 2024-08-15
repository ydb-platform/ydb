UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    utf8_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/yql_common/utils
)

END()
