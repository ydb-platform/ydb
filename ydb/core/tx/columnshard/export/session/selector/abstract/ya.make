LIBRARY()

SRCS(
    selector.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/protos
    ydb/services/bg_tasks/abstract
    ydb/library/conclusion
    ydb/core/tx/datashard
    ydb/core/protos
)

END()
