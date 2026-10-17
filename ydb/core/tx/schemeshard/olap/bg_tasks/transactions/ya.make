LIBRARY()

SRCS(
    tasks_list.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/tx/columnshard/bg_tasks/abstract
    ydb/core/tx/schemeshard/olap/bg_tasks/protos
)

END()
