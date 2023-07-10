LIBRARY()

SRCS(
    events.cpp
    proxy.cpp
    proxy_actor.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/erasure
    ydb/core/kesus/tablet
    ydb/core/scheme
    ydb/core/tx/scheme_cache
    ydb/library/services
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
