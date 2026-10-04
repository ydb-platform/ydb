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
RESOURCE(dashboard.js inmemory-metrics/dashboard.js)

END()
RECURSE_FOR_TESTS(ut)
