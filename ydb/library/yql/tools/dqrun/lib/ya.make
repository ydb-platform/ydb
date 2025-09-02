LIBRARY()

SRCS(
    dqrun_lib.cpp
)

PEERDIR(
    yt/yql/tools/ytrun/lib
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/comp_nodes/dq
    yt/yql/providers/yt/mkql_dq

    yql/essentials/providers/common/provider
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/common/metrics
    yql/essentials/core/cbo
    yql/essentials/core/dq_integration
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/computation
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/tools/yql_facade_run
    yql/essentials/sql/settings
    yql/essentials/utils/log

    ydb/core/fq/libs/row_dispatcher
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/shared_resources/interface
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/init
    ydb/core/fq/libs/actors

    ydb/library/yql/providers/clickhouse/provider
    ydb/library/yql/providers/clickhouse/actors

    ydb/library/yql/providers/ydb/provider
    ydb/library/yql/providers/ydb/comp_nodes
    ydb/library/yql/providers/ydb/actors

    ydb/library/yql/providers/s3/provider
    ydb/library/yql/providers/s3/actors

    ydb/library/yql/providers/pq/provider
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/library/yql/providers/pq/gateway/native
    ydb/library/yql/providers/pq/async_io

    ydb/library/yql/providers/solomon/provider
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/solomon/actors

    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/generic/provider
    ydb/library/yql/providers/generic/actors

    ydb/library/yql/providers/dq/interface
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/providers/dq/provider/exec
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/helper

    ydb/library/yql/providers/yt/dq_task_preprocessor
    ydb/library/yql/providers/yt/actors

    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/common/db_id_async_resolver

    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/transform
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/actors/input_transforms

    ydb/library/yql/utils/bindings
    ydb/library/yql/utils/actor_system

    ydb/library/actors/core
    ydb/library/actors/http
)

YQL_LAST_ABI_VERSION()

END()
