LIBRARY()

SRCS(
    pq_checkpoint_provider_integration.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/fq/libs/checkpointing
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/proto
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/adapters/issue
    ydb/services/scheme_secret
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
