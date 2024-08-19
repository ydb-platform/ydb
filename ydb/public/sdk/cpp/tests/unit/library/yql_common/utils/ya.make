UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    utf8_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/yql_common/utils
)

END()
