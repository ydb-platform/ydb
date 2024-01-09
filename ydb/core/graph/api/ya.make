LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    events.h
    service.h
    shard.h
)

PEERDIR(
    ydb/core/graph/protos
)

END()
