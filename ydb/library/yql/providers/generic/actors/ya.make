LIBRARY()

SRCS(
    yql_generic_read_actor.cpp
    yql_generic_source_factory.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/structured_token
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/generic/proto
    ydb/library/yql/public/types
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/public/sdk/cpp/client/ydb_types/credentials
)

YQL_LAST_ABI_VERSION()

END()
