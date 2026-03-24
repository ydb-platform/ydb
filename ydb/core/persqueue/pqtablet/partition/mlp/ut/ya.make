UNITTEST_FOR(ydb/core/persqueue/pqtablet/partition/mlp)

YQL_LAST_ABI_VERSION()

FORK_SUBTESTS()
SPLIT_FACTOR(200)

SIZE(MEDIUM)

#TIMEOUT(30)

SRCS(
    mlp_commit_ut.cpp
    mlp_consumer_split_ut.cpp
    mlp_consumer_ut.cpp
    mlp_counters_ut.cpp
    mlp_dlq_mover_ut.cpp
    mlp_storage_ut.cpp
    mlp_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/public/mlp/ut/common
    ydb/core/persqueue/ut/common
    library/cpp/iterator
)

END()
