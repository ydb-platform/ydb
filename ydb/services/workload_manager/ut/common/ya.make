LIBRARY()

SRCS(
    query_classifier_ut_common.h
    workload_service_ut_common.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/executer_actor
    ydb/core/kqp/query_data
    ydb/core/kqp/ut/common
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/testlib/actors
    ydb/core/tx/scheme_cache
    ydb/library/aclib
    ydb/library/yql/providers/common/http_gateway/ut_helpers
    ydb/library/yql/utils/actor_log
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/services/metadata
    ydb/services/workload_manager
    ydb/services/workload_manager/actors
    ydb/services/workload_manager/common
    ydb/services/workload_manager/metadata_subscription/resource_pool_classifier
    ydb/services/workload_manager/service
    ydb/services/workload_manager/tables
)

YQL_LAST_ABI_VERSION()

END()
