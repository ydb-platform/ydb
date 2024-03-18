LIBRARY()

SRCS(
    GLOBAL storage.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/session/selector/abstract
    ydb/core/tx/columnshard/blobs_action/abstract
    ydb/core/tx/columnshard/blobs_action/tier
    ydb/core/wrappers
)

END()
