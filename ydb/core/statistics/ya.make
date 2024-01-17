LIBRARY()

OWNER(
    monster
    g:kikimr
)

SRCS(
    events.h
    stat_service.h
    stat_service.cpp
)

PEERDIR(
    util
    ydb/library/actors/core
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
