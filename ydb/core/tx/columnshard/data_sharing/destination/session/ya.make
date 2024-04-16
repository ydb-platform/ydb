LIBRARY()

SRCS(
    destination.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/initiator/controller
    ydb/core/tx/columnshard/data_sharing/common/session
    ydb/core/tx/columnshard/data_sharing/common/transactions
    ydb/core/tx/columnshard/data_sharing/destination/transactions
)

END()
