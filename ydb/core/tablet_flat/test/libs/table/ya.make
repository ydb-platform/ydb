LIBRARY()

SRCS(
    misc.cpp
)

PEERDIR(
    ydb/core/tablet_flat/test/libs/rows
    ydb/core/tablet_flat/test/libs/table/model
    ydb/core/tablet_flat
)

END()

RECURSE(
    model
)
