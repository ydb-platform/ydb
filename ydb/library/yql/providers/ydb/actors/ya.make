LIBRARY()

SRCS(
    yql_ydb_read_actor.cpp
    yql_ydb_source_factory.cpp
)

PEERDIR(
    ydb/core/scheme
    yql/essentials/minikql/computation
    ydb/library/yql/providers/common/token_accessor/client
    yql/essentials/public/types
    yql/essentials/utils/log
    ydb/public/lib/experimental
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/driver
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/ydb/proto
)

YQL_LAST_ABI_VERSION()

END()
