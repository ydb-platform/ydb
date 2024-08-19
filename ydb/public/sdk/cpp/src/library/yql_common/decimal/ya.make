LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    yql_decimal.h
    yql_decimal.cpp
    yql_decimal_serialize.h
    yql_decimal_serialize.cpp
)

END()
