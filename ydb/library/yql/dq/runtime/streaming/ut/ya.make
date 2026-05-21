UNITTEST_FOR(ydb/library/yql/dq/runtime/streaming)

SRCS(
    dq_source_watermark_tracker_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/dq/runtime/streaming
)

END()
