YQL_LIBRARY()

# gn: move into parent

SRCS(
    resource_pool_classifiers.h
    resource_pool_classifiers.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
)
# gn: peerdir ydb/services/workload_manager/actors:scheme_actors

END()
