LIBRARY()

SRCS(
    actor_persqueue_client_iface.cpp
)

PEERDIR(
    ydb/core/persqueue/public
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/iam
)

END()

