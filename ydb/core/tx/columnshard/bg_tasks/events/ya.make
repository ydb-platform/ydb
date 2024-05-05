LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/columnshard/bg_tasks/protos
)

END()
