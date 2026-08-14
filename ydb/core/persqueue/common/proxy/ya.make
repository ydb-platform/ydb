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
    ydb/services/scheme_secret
=======
    ydb/public/sdk/cpp/src/client/iam
>>>>>>> f3674463da7 (Reduce msgbus usage in PQ tests: create/alter/delete via Topic/PersQueue SDK (#49687))
)

END()

