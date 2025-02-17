LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    persqueue.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_persqueue_core/impl
    ydb/public/sdk/cpp/client/ydb_persqueue_public
)

END()
