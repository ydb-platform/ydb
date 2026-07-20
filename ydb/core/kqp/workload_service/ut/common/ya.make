LIBRARY()

SRCS(
    kqp_query_classifier_ut_common.h
    kqp_workload_service_ut_common.cpp
)

PEERDIR(
    ydb/core/kqp/gateway/behaviour/resource_pool_classifier
    ydb/core/kqp/ut/common
    ydb/services/metadata
)

YQL_LAST_ABI_VERSION()

END()
