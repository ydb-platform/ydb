LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_types/credentials
    ydb/public/sdk/cpp/client/ydb_types/credentials/login
)

SRCS(
    ydb_proxy.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
