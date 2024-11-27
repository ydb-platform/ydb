LIBRARY()

PEERDIR(
    ydb/library/actors/core
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/computation
    yql/essentials/utils
)

SRCS(
    yql_common_dq_transform.cpp
)

YQL_LAST_ABI_VERSION()


END()
