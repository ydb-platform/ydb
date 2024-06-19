LIBRARY()

SRCS(
    events.h
    stat_service.h
    stat_service.cpp
    save_load_stats.h
    save_load_stats.cpp
)

PEERDIR(
    util
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/core/protos
    ydb/core/scheme
)

END()

RECURSE(
    aggregator
)

RECURSE_FOR_TESTS(
    ut
)
