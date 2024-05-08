LIBRARY()

SRCS(
    table.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tablet_flat
    ydb/core/tx/schemeshard/olap/operations/alter/abstract
)

END()
