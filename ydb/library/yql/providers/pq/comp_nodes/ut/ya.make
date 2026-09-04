UNITTEST_FOR(ydb/library/yql/providers/pq/comp_nodes)

SRCS(
    dq_pq_parsing_wrapper_ut.cpp
)

PEERDIR(
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/comp_nodes/ut/utils
    yql/essentials/public/udf/service/exception_policy

    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
