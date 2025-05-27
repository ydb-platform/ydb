LIBRARY()

SRCS(
    yql_generic_read_actor.cpp
    yql_generic_lookup_actor.cpp
    yql_generic_provider_factories.cpp
    yql_generic_token_provider.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/compute
    yql/essentials/minikql/computation
    yql/essentials/providers/common/structured_token
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/generic/proto
    yql/essentials/public/types
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
