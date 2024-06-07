LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/statistics/abstract
    ydb/services/bg_tasks/abstract
)

END()
