LIBRARY()

SRCS(
    yql_pq_dq_transform.cpp
)

PEERDIR(
    ydb/library/yql/providers/pq/task_meta

    yql/essentials/minikql
)

YQL_LAST_ABI_VERSION()

END()
