LIBRARY()

SRCS(
    yql_yt_lookup_actor.cpp
    yql_yt_provider_factories.cpp
)

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/yt/proto
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/public/types
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)