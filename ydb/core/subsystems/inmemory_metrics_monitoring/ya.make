LIBRARY()
SRCS(
    subsystem.cpp
    viewer.cpp
)
PEERDIR(
    library/cpp/json
    ydb/library/actors/core
    ydb/library/actors/core/subsystems
)
RESOURCE(overview.js inmemory-metrics/overview.js)

END()
RECURSE_FOR_TESTS(ut)

RECURSE(metric_chart)
