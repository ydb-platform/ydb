LIBRARY()

PEERDIR(
    library/cpp/actors/core
    ydb/library/yql/dq/integration/transform
    ydb/library/yql/minikql/computation
    ydb/library/yql/minikql
    ydb/library/yql/utils
)

SRCS(
    yql_common_dq_transform.cpp
)

YQL_LAST_ABI_VERSION()


END()
