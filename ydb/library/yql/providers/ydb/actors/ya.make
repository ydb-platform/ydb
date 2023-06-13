LIBRARY()

SRCS(
    yql_ydb_read_actor.cpp
    yql_ydb_source_factory.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/public/types
    ydb/library/yql/utils/log
    ydb/public/lib/experimental
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/ydb/proto
)

YQL_LAST_ABI_VERSION()

END()
