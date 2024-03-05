LIBRARY()

SRCS(
    transfer.cpp
    status.cpp
    control.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/data_sharing/destination/session
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/library/actors/core
)

END()
