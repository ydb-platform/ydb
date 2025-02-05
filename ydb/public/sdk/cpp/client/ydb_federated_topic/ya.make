LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h)

SRCS(
    federated_topic.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic
    ydb/public/sdk/cpp/client/ydb_federated_topic/impl
)

END()
