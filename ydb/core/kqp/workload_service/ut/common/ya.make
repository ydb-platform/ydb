LIBRARY()

SRCS(
    kqp_query_classifier_ut_common.h
    kqp_workload_service_ut_common.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
)

YQL_LAST_ABI_VERSION()

END()
