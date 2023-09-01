LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_query
)

YQL_LAST_ABI_VERSION()

END()
