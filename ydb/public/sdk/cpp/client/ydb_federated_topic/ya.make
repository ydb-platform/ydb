LIBRARY()

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h)

SRCS(
    federated_topic.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_federated_topic/impl
)

END()

RECURSE_FOR_TESTS(
    ut
)
