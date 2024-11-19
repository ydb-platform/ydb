LIBRARY()

SRCS(
    common.h
    events.h
)

PEERDIR(
    util
    ydb/library/actors/core
    ydb/library/query_actor
    ydb/library/minsketch
    ydb/core/protos
    ydb/core/scheme
)

END()

RECURSE(
    aggregator
    database
    service
    ut_common
)

RECURSE_FOR_TESTS(
    aggregator/ut
    database/ut
    service/ut
)
