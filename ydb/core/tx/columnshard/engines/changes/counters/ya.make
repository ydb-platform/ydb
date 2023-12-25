LIBRARY()

OWNER(
    g:kikimr
)

SRCS(
    general.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/actors/core
    ydb/core/tablet_flat
)

END()
