PROGRAM(bridge_skipper)

SRCS(main.cpp)

PEERDIR(
    library/cpp/json
    library/cpp/monlib/dynamic_counters
    library/cpp/svnversion
    ydb/apps/bridge_skipper/config
    ydb/apps/bridge_skipper/http
    ydb/apps/bridge_skipper/runtime
    ydb/library/actors/core
    ydb/library/actors/struct_log
    ydb/library/actors/http
    ydb/library/actors/interconnect/poller
)

END()

RECURSE(
    acceptance
    config
    domain
    http
    runtime
    ydbd_mock
)
