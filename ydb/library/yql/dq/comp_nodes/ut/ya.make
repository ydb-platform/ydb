UNITTEST_FOR(ydb/library/yql/dq/comp_nodes)

SIZE(MEDIUM)

PEERDIR(
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/comp_nodes/ut/utils
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy

    ydb/core/kqp/runtime

    library/cpp/testing/unittest
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry

)

YQL_LAST_ABI_VERSION()

SRCS(

    dq_hash_combine_ut.cpp
    dq_block_hash_join_ut.cpp
    dq_scalar_hash_join_ut.cpp
)

END()
