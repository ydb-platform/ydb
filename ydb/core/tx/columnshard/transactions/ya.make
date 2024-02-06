LIBRARY()

SRCS(
    tx_controller.cpp
    locks_db.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/data_events
)

YQL_LAST_ABI_VERSION()

END()
