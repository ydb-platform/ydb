LIBRARY()

SRCS(
    events.h
)

PEERDIR(
    util
    ydb/library/actors/core
    ydb/library/query_actor
    yql/essentials/core/minsketch
    yql/essentials/core/histogram
    ydb/core/protos
    ydb/core/scheme
)

END()

RECURSE(
    aggregator
    common
    database
    service
    ut_common
)

RECURSE_FOR_TESTS(
    aggregator/ut
    database/ut
    service/ut
    ut
)
