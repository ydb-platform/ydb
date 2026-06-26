LIBRARY()

SRCS(
    describe_schema_secrets_service.cpp
)

PEERDIR(
    library/cpp/retry
    library/cpp/threading/future
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/protos
    ydb/core/tx/scheme_board
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/library/aclib
    ydb/library/actors/core
    ydb/services/metadata/secret
    ydb/services/metadata
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
