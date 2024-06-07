LIBRARY()

SRCS(
    data_plane.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/fq/libs/events
    ydb/core/fq/libs/row_dispatcher/protos
)

END()
