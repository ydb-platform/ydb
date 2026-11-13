UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

INCLUDE(${ARCADIA_ROOT}/ydb/core/kqp/ut/federated_query/datastreams/common.inc)

# Keep only the YDB recipe for scenarios without external-service emulators.
SRCS(
    datastreams_checkpoints_ut.cpp
    datastreams_queries_restart_ut.cpp
    datastreams_table_mode_ut.cpp
    kqp_has_path_ut.cpp
    streaming_aggregation_ut.cpp
    streaming_deferrd_commit_write_ut.cpp
    streaming_operation_continuation_ut.cpp
    streaming_sys_view_ut.cpp
)

END()
