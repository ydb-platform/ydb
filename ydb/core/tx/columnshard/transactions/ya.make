LIBRARY()

SRCS(
    tx_controller.cpp
    propose_transaction_base.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    ydb/core/tx/data_events
    ydb/core/tx/columnshard/data_sharing/destination/events
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
