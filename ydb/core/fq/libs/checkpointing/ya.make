LIBRARY()

SRCS(
    checkpoint_coordinator.cpp
    checkpoint_coordinator.h
    checkpoint_id_generator.cpp
    checkpoint_id_generator.h
    pending_checkpoint.cpp
    pending_checkpoint.h
    utils.cpp
    utils.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/checkpointing_common
    ydb/core/fq/libs/checkpoint_storage/events
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/state
    ydb/library/yql/providers/dq/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
