LIBRARY()

SRCS(
    transfer.cpp  # gn: into ydb/core/tx/columnshard
    status.cpp  # gn: into ydb/core/tx/columnshard
    control.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/data_sharing/destination/session
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/library/actors/core
)

END()
