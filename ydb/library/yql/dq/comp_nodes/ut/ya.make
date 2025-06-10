UNITTEST_FOR(ydb/library/yql/dq/comp_nodes)

SIZE(MEDIUM)

PEERDIR(
    ydb/library/yql/dq/comp_nodes
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy

    library/cpp/testing/unittest
    ydb/core/kqp/runtime
)

YQL_LAST_ABI_VERSION()

SRCS(
    dq_block_hash_join_ut.cpp
)

END() 
