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
    ydb/core/fq/libs/db_schema
    ydb/core/fq/libs/shared_resources/interface
    ydb/core/protos
    ydb/library/mkql_proto
    ydb/library/yql/ast
    ydb/library/yql/core/facade
    ydb/library/yql/core/services/mounts
    ydb/library/yql/dq/integration/transform
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/providers/clickhouse/provider
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema/mkql
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/worker_manager/interface
    ydb/library/yql/providers/ydb/provider
    ydb/library/yql/public/issue
    ydb/library/yql/public/issue/protos
    ydb/library/yql/sql/settings
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
