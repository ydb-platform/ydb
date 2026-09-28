LIBRARY()

SRCS(
    kqp_execution_rendering.cpp
    kqp_scan_rendering.cpp
    kqp_shard_rendering.cpp
    kqp_query_rendering.cpp
    kqp_task_rendering.cpp
)

PEERDIR(
    library/cpp/time_provider
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/library/actors/wilson
    ydb/library/wilson_ids
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/actors/protos
    ydb/library/security
    ydb/public/api/protos
    yql/essentials/ast
)

END()

RECURSE_FOR_TESTS(
    ut
)
