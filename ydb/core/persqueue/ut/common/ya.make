LIBRARY()

SRCS(
    pq_ut_common.cpp
    pq_ut_common.h

    autoscaling_ut_common.cpp
    autoscaling_ut_common.h
)

PEERDIR(
    ydb/core/persqueue
    ydb/core/testlib
    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
