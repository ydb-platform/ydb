LIBRARY()

SRCS(
    tx_scan.cpp
    tx_internal_scan.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/reader/abstract
    ydb/core/tablet_flat
    ydb/core/tx/columnshard/engines/reader/actor
    ydb/core/tx/columnshard/engines/reader/sys_view/constructor
    ydb/core/tx/columnshard/engines/reader/plain_reader/constructor
)

END()
