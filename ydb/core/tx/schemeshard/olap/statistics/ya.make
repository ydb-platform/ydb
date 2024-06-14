LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/services/bg_tasks/abstract
    ydb/core/tx/schemeshard/olap/common
    ydb/core/tx/columnshard/engines/scheme/statistics/abstract
    ydb/core/protos
)

END()
