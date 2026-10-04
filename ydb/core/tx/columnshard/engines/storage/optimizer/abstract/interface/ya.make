LIBRARY()

SRCDIR(ydb/core/tx/columnshard/engines/storage/optimizer/abstract)

SRCS(
    optimizer.h
)

PEERDIR(
    ydb/core/base
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/counters
    ydb/library/accessor
    ydb/library/conclusion
    ydb/services/bg_tasks/abstract
)

END()
