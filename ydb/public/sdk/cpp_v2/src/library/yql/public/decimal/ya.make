LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    yql_decimal.h
    yql_decimal.cpp
    yql_decimal_serialize.h
    yql_decimal_serialize.cpp
)

END()
