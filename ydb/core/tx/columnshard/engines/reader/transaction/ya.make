LIBRARY()

SRCS(
    common.cpp
    scan_gate.cpp
    tx_scan.cpp
    tx_internal_scan.cpp
)

PEERDIR(
    ydb/core/kqp/common/events
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tx/columnshard/engines/reader/actor
    ydb/core/tx/columnshard/engines/reader/common
)

END()
