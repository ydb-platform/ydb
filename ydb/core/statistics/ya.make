YQL_LIBRARY()

SRCS(
    events.h
    service_id.cpp
    service_id.h
)

PEERDIR(
    util
    ydb/core/protos
    ydb/core/scheme
    ydb/library/actors/core
    ydb/library/query_actor
    yql/essentials/core/histogram
    yql/essentials/core/minsketch
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
