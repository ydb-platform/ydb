YQL_LIBRARY()

SRCS(
    cpu_quota_manager.cpp
    events.cpp
    helpers.cpp
    service_id.cpp
)

PEERDIR(
    library/cpp/retry
    ydb/core/base
    ydb/core/resource_pools
    ydb/core/scheme
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/public/sdk/cpp/src/client/types
)

END()

RECURSE_FOR_TESTS(
    ut
)
