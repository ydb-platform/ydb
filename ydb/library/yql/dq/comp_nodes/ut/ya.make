UNITTEST_FOR(ydb/library/yql/dq/comp_nodes)

SIZE(MEDIUM)

PEERDIR(
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    ydb/library/yql/dq/comp_nodes
)

YQL_LAST_ABI_VERSION()

SRCS(
    dq_block_hash_join_ut.cpp
    dq_block_hash_join_ut_utils.cpp
)

END() 
