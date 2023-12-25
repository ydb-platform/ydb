LIBRARY()

PEERDIR(
    ydb/core/scheme
    ydb/library/mkql_proto/protos
    ydb/library/yql/dq/actors/protos
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/structured_token
    ydb/library/yql/providers/ydb/proto
    ydb/public/lib/experimental
    ydb/public/sdk/cpp/client/ydb_driver
)

SRCS(
    yql_kik_scan.cpp
    yql_ydb_factory.cpp
    yql_ydb_dq_transform.cpp
)

YQL_LAST_ABI_VERSION()

END()
