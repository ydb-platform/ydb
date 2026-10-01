LIBRARY()

SRCS(
    memory_quota.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/library/yql/dq/actors/compute
    yql/essentials/minikql
)

YQL_LAST_ABI_VERSION()

END()
