LIBRARY()

SRCS(
    selector.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/export/protos
    ydb/services/bg_tasks/abstract
    ydb/library/conclusion
)

END()
