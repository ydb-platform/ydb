LIBRARY()

SRCS(
    table_queries.cpp
)

PEERDIR(
    ydb/services/workload_manager/common

    ydb/library/query_actor

    ydb/library/table_creator
)

YQL_LAST_ABI_VERSION()

END()
