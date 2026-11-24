YQL_LIBRARY()

SRCS(
    progress.cpp
    query_stats.cpp
    ru_calc.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/yql/dq/actors/protos
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
