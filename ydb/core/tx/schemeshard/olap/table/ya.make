LIBRARY()

SRCS(
    table.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/protos
    ydb/core/tablet_flat
)

END()
