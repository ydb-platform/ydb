UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_acl_ut.cpp
    kqp_scheme_ut.cpp
    kqp_constraints_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/kqp/workload_service/ut/common
    ydb/core/tx/columnshard/hooks/testing
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
