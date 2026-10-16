UNITTEST_FOR(ydb/core/subsystems/inmemory_metrics_monitoring)
REQUIREMENTS(cpu:1)
SRCS(viewer_ut.cpp)
PEERDIR(
    ydb/library/actors/testlib
)
END()
