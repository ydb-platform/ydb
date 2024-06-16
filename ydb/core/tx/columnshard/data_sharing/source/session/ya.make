LIBRARY()

SRCS(
    source.cpp
    cursor.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/common/session
    ydb/core/tx/columnshard/data_sharing/destination/events
    ydb/core/tx/columnshard/data_sharing/source/transactions
)

END()
