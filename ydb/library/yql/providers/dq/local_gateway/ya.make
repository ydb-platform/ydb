LIBRARY()

OWNER(g:yql)

YQL_LAST_ABI_VERSION()

SRCS(
    yql_dq_gateway_local.cpp
)

PEERDIR(
    ydb/library/yql/utils 
    ydb/library/yql/dq/actors/compute 
    ydb/library/yql/providers/dq/provider 
    ydb/library/yql/providers/dq/api/protos 
    ydb/library/yql/providers/dq/task_runner 
    ydb/library/yql/providers/dq/worker_manager
    ydb/library/yql/providers/pq/async_io 
    ydb/library/yql/providers/s3/actors 
    ydb/library/yql/providers/ydb/actors 
    ydb/library/yql/providers/clickhouse/actors 
    ydb/library/yql/providers/dq/service
)

END()
