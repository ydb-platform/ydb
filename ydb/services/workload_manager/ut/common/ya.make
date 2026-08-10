LIBRARY()

SRCS(
    query_classifier_ut_common.h
    workload_service_ut_common.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/services/metadata
    ydb/services/workload_manager/metadata_subscription/resource_pool_classifier
)

YQL_LAST_ABI_VERSION()

END()
