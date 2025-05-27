LIBRARY()

ENV(YDB_USE_IN_MEMORY_PDISKS=true)

ENV(YDB_ERASURE=block_4-2)

ENV(YDB_FEATURE_FLAGS="enable_topic_transfer")
ENV(YDB_GRPC_SERVICES="replication")

PEERDIR(
    library/cpp/threading/local_executor
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/draft
)

SRCS(
    utils.cpp
)

END()
