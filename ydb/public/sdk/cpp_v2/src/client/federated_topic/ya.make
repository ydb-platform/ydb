LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h)

SRCS(
    ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/topic
    ydb/public/sdk/cpp_v2/src/client/federated_topic/impl
)

END()
