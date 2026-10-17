LIBRARY()

SRCS(
    common.cpp  # gn: into ydb/core/tx/columnshard
    tx_scan.cpp  # gn: into ydb/core/tx/columnshard
    tx_internal_scan.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/engines/reader/actor
    ydb/core/tx/columnshard/engines/reader/common
)

END()
