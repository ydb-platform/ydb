LIBRARY()

SRCS(
    diagnostics.cpp
    state_file.cpp
    ydb_clients.cpp
    skipper.cpp
)

PEERDIR(
    library/cpp/json
    ydb/apps/bridge_skipper/config
    ydb/apps/bridge_skipper/domain
    ydb/library/actors/core
    ydb/library/actors/struct_log
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/monitoring
    ydb/public/sdk/cpp/src/client/proto
)

END()

RECURSE_FOR_TESTS(ut)
