YQL_LIBRARY()

SRCS(
    table_queries.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/simple
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/library/table_creator
    ydb/services/workload_manager/common
)

END()
