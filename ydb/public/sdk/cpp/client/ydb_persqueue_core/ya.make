LIBRARY()

SRCS(
    persqueue.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_persqueue_core/impl
    ydb/public/sdk/cpp/client/ydb_persqueue_public
)

END()

RECURSE_FOR_TESTS(
    ut
)
