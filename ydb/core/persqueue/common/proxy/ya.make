LIBRARY()

SRCS(
    actor_persqueue_client_iface.cpp
    mirror_credentials.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/iam
    ydb/services/scheme_secret
)

END()

