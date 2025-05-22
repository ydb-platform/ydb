LIBRARY()

SRCS(
    status_channel.cpp
    task.cpp
    session.cpp
    actor.cpp
    common.cpp
)

PEERDIR(
    ydb/core/tx/schemeshard/olap/bg_tasks/protos
    ydb/core/tx/columnshard/bg_tasks/abstract
)

END()
