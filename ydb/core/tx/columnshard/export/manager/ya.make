LIBRARY()

SRCS(
    manager.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/export/session
    ydb/core/tx/columnshard/export/protos
)

END()
