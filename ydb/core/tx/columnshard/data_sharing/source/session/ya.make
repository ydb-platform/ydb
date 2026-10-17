LIBRARY()

SRCS(
    source.cpp  # gn: into ydb/core/tx/columnshard
    cursor.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/common/session
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/data_sharing/source/transactions
)

END()
