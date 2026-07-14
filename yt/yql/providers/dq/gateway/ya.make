LIBRARY()

SRCS(
    yql_dq_gateway.cpp
    yql_dq_gateway_factory.h
)

PEERDIR(
    library/cpp/threading/future
    library/cpp/threading/task_scheduler
    library/cpp/yson/node
    library/cpp/yson
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/api/grpc
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/provider
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/providers/common/provider
    yql/essentials/public/issue
    yql/essentials/utils/backtrace
    yql/essentials/utils/failure_injector
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
