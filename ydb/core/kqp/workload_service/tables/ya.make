LIBRARY()

SRCS(
    table_queries.cpp
)

PEERDIR(
    ydb/core/kqp/workload_service/common

    ydb/library/query_actor

    ydb/library/table_creator
)

YQL_LAST_ABI_VERSION()

END()
