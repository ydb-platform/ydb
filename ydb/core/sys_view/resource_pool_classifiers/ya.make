YQL_LIBRARY()

SRCS(
    resource_pool_classifiers.h
    resource_pool_classifiers.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/core/sys_view/scan
    ydb/library/actors/core
    ydb/services/workload_manager/metadata_subscription/resource_pool_classifier
)

END()
