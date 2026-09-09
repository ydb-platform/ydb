LIBRARY()

SRCS(
    memory_quota.cpp
)

PEERDIR(
    ydb/library/yql/dq/actors/compute
    yql/essentials/minikql
)

YQL_LAST_ABI_VERSION()

END()
