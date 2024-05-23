LIBRARY()

SRCS(
    global.cpp
    common.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/bg_tasks/abstract
    ydb/core/tx/schemeshard/olap/bg_tasks/protos
)

END()
