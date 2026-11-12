YQL_LIBRARY()

SRCS(
    yql_generic_match_predicate.cpp
)

PEERDIR(
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
