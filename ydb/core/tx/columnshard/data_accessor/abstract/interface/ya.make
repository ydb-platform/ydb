LIBRARY()

SRCDIR(ydb/core/tx/columnshard/data_accessor/abstract)

SRCS(
    constructor.h
)

PEERDIR(
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/accessor
    ydb/library/conclusion
    ydb/services/bg_tasks/abstract
)

END()
