LIBRARY()

SRCS(
    data_plane.cpp
)

PEERDIR(
    ydb/core/fq/libs/events
    ydb/core/fq/libs/row_dispatcher/protos
    ydb/library/actors/core
    ydb/library/yql/providers/pq/provider
)

END()
