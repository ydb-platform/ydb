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
    ydb/library/actors/core
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_storage
    ydb/core/fq/libs/ydb
    ydb/core/fq/libs/checkpoint_storage/events
    ydb/core/fq/libs/checkpoint_storage/proto
    ydb/core/fq/libs/checkpointing_common
    ydb/core/fq/libs/shared_resources
    ydb/library/security
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/proto
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
