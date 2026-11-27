YQL_LIBRARY()

SRCS(
    table_checksum.cpp
)

PEERDIR(
    ydb/core/backup/common
    ydb/core/tx/long_tx_service/public
    ydb/core/util
    ydb/library/query_actor
)

END()
