LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/services/bg_tasks/abstract
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
)

END()
