LIBRARY()

OWNER(
    g:kikimr
    g:logbroker
)

SRCS(
    persqueue.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_persqueue/codecs
    ydb/public/sdk/cpp/client/ydb_persqueue_core
)

END()
