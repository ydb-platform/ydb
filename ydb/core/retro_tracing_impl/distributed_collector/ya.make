LIBRARY()

SRCS(
    distributed_retro_collector.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/nodewarden
    ydb/core/control/lib
    ydb/core/protos
    ydb/library/actors/retro_tracing
)

END()
