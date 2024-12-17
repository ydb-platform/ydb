LIBRARY()

SRCS(
    session.cpp
    storage.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/bg_tasks/abstract
    ydb/core/tx/columnshard/bg_tasks/protos
)

END()
