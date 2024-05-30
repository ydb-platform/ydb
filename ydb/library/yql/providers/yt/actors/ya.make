LIBRARY()

SRCS(
    yql_yt_lookup_actor.cpp
)

PEERDIR(
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/yt/proto
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/types
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)