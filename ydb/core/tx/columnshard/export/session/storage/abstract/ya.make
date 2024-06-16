LIBRARY()

SRCS(
    storage.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/session/storage/s3
    ydb/core/tx/columnshard/export/protos
    ydb/services/bg_tasks/abstract
    ydb/library/conclusion
    ydb/core/protos
)

END()
