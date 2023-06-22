UNITTEST_FOR(ydb/core/kqp/run_script_actor)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

SIZE(MEDIUM)

SRCS(
    kqp_run_script_actor_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_types/operation
)

YQL_LAST_ABI_VERSION()

END()