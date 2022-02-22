OWNER(g:yq)

LIBRARY()

SRCS(
    init.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/http
    ydb/core/base
    ydb/core/protos
    ydb/core/yq/libs/actors
    ydb/core/yq/libs/audit
    ydb/core/yq/libs/checkpoint_storage
    ydb/core/yq/libs/checkpointing
    ydb/core/yq/libs/control_plane_proxy
    ydb/core/yq/libs/control_plane_storage
    ydb/core/yq/libs/events
    ydb/core/yq/libs/gateway
    ydb/core/yq/libs/shared_resources
    ydb/core/yq/libs/test_connection
    ydb/library/folder_service
    ydb/library/folder_service/proto
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/utils/actor_log
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/providers/clickhouse/actors
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/providers/dq/worker_manager
    ydb/library/yql/providers/dq/worker_manager/interface
    ydb/library/yql/providers/pq/async_io
    ydb/library/yql/providers/pq/cm_client/interface
    ydb/library/yql/providers/pq/gateway/native
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/providers/s3/actors
    ydb/library/yql/providers/s3/provider
    ydb/library/yql/providers/solomon/async_io
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/solomon/proto
    ydb/library/yql/providers/solomon/provider
    ydb/library/yql/providers/ydb/actors
    ydb/library/yql/providers/ydb/comp_nodes
    ydb/library/yql/providers/ydb/provider
)

YQL_LAST_ABI_VERSION()

END()
