LIBRARY()

PEERDIR(
    ydb/core/scheme
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/structured_token
    ydb/public/lib/experimental
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/library/yql/providers/ydb/proto
)

SRCS(
    yql_kik_scan.cpp
    yql_ydb_factory.cpp
    yql_ydb_dq_transform.cpp
)

YQL_LAST_ABI_VERSION()

END()
