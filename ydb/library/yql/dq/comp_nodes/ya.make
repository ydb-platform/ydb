LIBRARY()

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/minikql/computation
    ydb/library/yql/utils
    ydb/library/yql/dq/actors/spilling
)

SRCS(
    yql_common_dq_factory.cpp
)

YQL_LAST_ABI_VERSION()


END()

