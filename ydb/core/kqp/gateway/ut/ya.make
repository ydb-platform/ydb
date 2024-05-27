GTEST()

SRCS(
    metadata_conversion.cpp
)

PEERDIR(
    ydb/core/kqp/gateway
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf/service/stub
    ydb/services/metadata
)

YQL_LAST_ABI_VERSION()

END()

