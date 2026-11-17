LIBRARY()

SRCS(
    actor_persqueue_client_iface.cpp
    mirror_credentials.cpp
)

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/public/sdk/cpp/src/client/topic
<<<<<<< HEAD
=======
    ydb/public/sdk/cpp/src/client/iam
    ydb/services/scheme_secret
>>>>>>> 155ca05c7e3 ([KIKIMR-27673] Support secret references in topic mirroring credentials (#59067))
)

END()

