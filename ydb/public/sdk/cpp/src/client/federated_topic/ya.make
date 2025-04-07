LIBRARY()

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h)

SRCS(
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/federated_topic/impl
    ydb/public/sdk/cpp/src/client/federated_topic/ut/fds_mock
)

END()

RECURSE_FOR_TESTS(
    ut
)
