LIBRARY()

PEERDIR(
    ydb/core/scheme
    ydb/library/mkql_proto/protos
    ydb/library/yql/dq/actors/protos
    yql/essentials/minikql/computation
    yql/essentials/providers/common/structured_token
    ydb/library/yql/providers/ydb/proto
    ydb/public/lib/experimental
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/driver
)

SRCS(
    yql_kik_scan.cpp
    yql_ydb_factory.cpp
    yql_ydb_dq_transform.cpp
)

YQL_LAST_ABI_VERSION()

END()
