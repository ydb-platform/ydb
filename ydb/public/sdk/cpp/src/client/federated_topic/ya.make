LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h)

SRCS(
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/federated_topic/impl
)

END()
