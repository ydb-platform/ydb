LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/s3/actors_factory
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_query
)

YQL_LAST_ABI_VERSION()

END()
