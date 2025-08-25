LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/types/credentials
    ydb/public/sdk/cpp/src/client/types/credentials/login
)

SRCS(
    topic_message.cpp
    ydb_proxy.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    local_proxy
)

RECURSE_FOR_TESTS(
    ut
)
