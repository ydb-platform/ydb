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
    local_proxy/local_partition_actor.cpp
    local_proxy/local_partition_committer.cpp
    local_proxy/local_partition_reader.cpp
    local_proxy/local_proxy.cpp
    topic_message.cpp
    ydb_proxy.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
