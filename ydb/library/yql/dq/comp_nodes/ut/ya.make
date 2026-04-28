UNITTEST_FOR(ydb/library/yql/dq/comp_nodes)

PEERDIR(
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/comp_nodes/ut/utils
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy

    library/cpp/testing/unittest
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
)

IF (SANITIZER_TYPE)
    TIMEOUT(1800)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(
    dq_hash_combine_ut.cpp
)

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "ydb")
PEERDIR(
    ydb/core/kqp/tools/join_perf
)

SRCS(
    dq_hash_join_ut.cpp
)
ENDIF()

END()
