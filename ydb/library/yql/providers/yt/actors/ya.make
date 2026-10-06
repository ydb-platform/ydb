YQL_LIBRARY()

SRCS(
    yql_yt_message_stream_source.cpp
    yql_yt_lookup_actor.cpp
    yql_yt_provider_factories.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/message_stream/async_io
    ydb/library/yql/providers/yt/proto
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/yt/gateway/clients/message_stream
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/providers/common/provider
    yt/yql/providers/yt/proto
    yt/yql/providers/yt/gateway/file
    ydb/library/yql/dq/actors/compute
    yql/essentials/public/types
)

END()

RECURSE_FOR_TESTS(ut)