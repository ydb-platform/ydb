LIBRARY()

SRCS(
    yql_yt_lookup_actor.cpp
    yql_yt_provider_factories.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/providers/common/provider
    ydb/library/yql/providers/yt/proto
    ydb/library/yql/providers/yt/gateway/file
    ydb/library/yql/dq/actors/compute
    yql/essentials/public/types
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)