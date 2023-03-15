LIBRARY()

SRCS(
    persqueue.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_persqueue_core
    ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
)

END()
