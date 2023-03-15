LIBRARY()

SRCS(
    gc.cpp
    storage_proxy.cpp
    storage_service.cpp
    ydb_checkpoint_storage.cpp
    ydb_state_storage.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/actors/core
    ydb/core/yq/libs/actors/logging
    ydb/core/yq/libs/control_plane_storage
    ydb/core/yq/libs/ydb
    ydb/core/yq/libs/checkpoint_storage/events
    ydb/core/yq/libs/checkpoint_storage/proto
    ydb/core/yq/libs/checkpointing_common
    ydb/core/yq/libs/shared_resources
    ydb/library/security
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/proto
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
    proto
)
