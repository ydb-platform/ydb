UNITTEST_FOR(ydb/library/yql/dq/actors/compute)

SRCS(
    dq_compute_actor_ut.cpp
    dq_compute_actor_async_input_helper_ut.cpp
    dq_compute_issues_buffer_ut.cpp
    dq_source_watermark_tracker_ut.cpp
    #retry_events_queue_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/dq/actors
    ydb/library/actors/wilson
    ydb/library/actors/testlib
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/minikql/comp_nodes/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
