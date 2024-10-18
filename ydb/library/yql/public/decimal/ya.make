LIBRARY()

SRCS(
    yql_decimal.h
    yql_decimal.cpp
    yql_decimal_serialize.h
    yql_decimal_serialize.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/yql_common/decimal
)

END()
