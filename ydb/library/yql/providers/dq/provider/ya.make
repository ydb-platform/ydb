LIBRARY()

OWNER(g:yql)

SRCS(
    yql_dq_control.cpp
    yql_dq_control.h
    yql_dq_datasink_type_ann.cpp 
    yql_dq_datasink_type_ann.h 
    yql_dq_datasource_type_ann.cpp 
    yql_dq_datasource_type_ann.h 
    yql_dq_gateway.cpp
    yql_dq_gateway.h
    yql_dq_provider.cpp
    yql_dq_provider.h
    yql_dq_datasink.cpp
    yql_dq_datasink.h
    yql_dq_datasource.cpp
    yql_dq_datasource.h
    yql_dq_recapture.cpp 
    yql_dq_recapture.h 
)

PEERDIR(
    library/cpp/grpc/client
    library/cpp/threading/task_scheduler
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/library/yql/core
    ydb/library/yql/dq/tasks
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/common/gateway
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/common/transform
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/backtrace
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/config
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/dq/interface
    ydb/library/yql/providers/dq/opt
    ydb/library/yql/providers/dq/planner
    ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION() 

END()
