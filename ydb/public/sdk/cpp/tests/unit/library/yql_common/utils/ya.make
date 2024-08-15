UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    utf8_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/yql_common/utils
)

END()
