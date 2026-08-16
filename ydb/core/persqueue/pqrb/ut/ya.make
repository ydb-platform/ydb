GTEST()

ADDINCL(
    ydb/public/sdk/cpp
)

SIZE(MEDIUM)

FORK_SUBTESTS()

SRCS(
    balancing_ut.cpp
    describes_ut.cpp
    graph_cmp_ut.cpp
    metrics_ut.cpp
    mlp_ut.cpp
    partition_scale_manager_graph_cmp_ut.cpp
    partitions_location_queue_ut.cpp
    scale_and_mirror_ut.cpp
    sdk_balancing_ut.cpp
    write_partition_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/pqrb
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/tx/tx_proxy
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
