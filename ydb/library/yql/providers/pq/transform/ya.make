LIBRARY()

SRCS(
    yql_pq_dq_transform.cpp
)

PEERDIR(
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/proto

    yql/essentials/minikql
)

YQL_LAST_ABI_VERSION()

END()
