LIBRARY()

SRCS(
    yql_mock.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/json/yson
    library/cpp/monlib/dynamic_counters
    library/cpp/random_provider
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    ydb/core/base
    ydb/core/fq/libs/actors
    ydb/core/fq/libs/common
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/db_schema
    ydb/core/fq/libs/shared_resources/interface
    ydb/core/protos
    ydb/library/mkql_proto
    yql/essentials/ast
    yql/essentials/core/facade
    yql/essentials/core/services/mounts
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/comp_nodes
    ydb/library/yql/providers/clickhouse/provider
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/mkql
    yql/essentials/providers/common/udf_resolve
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/worker_manager/interface
    ydb/library/yql/providers/ydb/provider
    yql/essentials/public/issue
    yql/essentials/public/issue/protos
    yql/essentials/sql/settings
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/table
)

YQL_LAST_ABI_VERSION()

END()
