UNITTEST_FOR(ydb/library/yql/dq/actors/compute)

SRCS(
    dq_compute_issues_buffer_ut.cpp
    dq_source_watermark_tracker_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/dq/actors
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

END()
