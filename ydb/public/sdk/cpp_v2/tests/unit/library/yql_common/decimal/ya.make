UNITTEST()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    yql_decimal_ut.cpp
    yql_wide_int_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/yql_common/decimal
)

END()
