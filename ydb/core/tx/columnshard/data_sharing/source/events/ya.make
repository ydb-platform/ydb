LIBRARY()

SRCS(
    transfer.cpp  # gn: into ydb/core/tx/columnshard
    control.cpp  # gn: into ydb/core/tx/columnshard
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/source/session
)

END()
