LIBRARY()

SRCS(
    actor.cpp
    metrics.cpp
    state_json.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/monlib/encode/prometheus
    library/cpp/monlib/metrics
    ydb/apps/bridge_skipper/runtime
    ydb/library/actors/core
    ydb/library/actors/http
)

END()

RECURSE_FOR_TESTS(ut)
