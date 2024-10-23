LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    federated_topic.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/federated_topic
)

END()
