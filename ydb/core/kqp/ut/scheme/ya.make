UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_acl_ut.cpp
    kqp_constraints_ut.cpp
    kqp_scheme_ut.cpp
    kqp_scheme_fulltext_ut.cpp
    kqp_scheme_type_info_ut.cpp
    kqp_user_management_ut.cpp
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
