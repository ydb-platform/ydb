LIBRARY()

SRCS(
    task_meta.cpp
)

PEERDIR(
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/pq/proto
)

YQL_LAST_ABI_VERSION()

END()
