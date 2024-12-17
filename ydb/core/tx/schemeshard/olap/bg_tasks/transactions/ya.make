LIBRARY()

SRCS(
    tasks_list.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/bg_tasks/abstract
    ydb/core/tx/schemeshard/olap/bg_tasks/protos
)

YQL_LAST_ABI_VERSION()

END()
