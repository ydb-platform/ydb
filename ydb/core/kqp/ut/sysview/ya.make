UNITTEST_FOR(ydb/core/kqp)

ADDINCL(
    ydb/public/sdk/cpp
)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    kqp_sys_col_ut.cpp
    kqp_sys_view_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/ydb_issue
    ydb/services/workload_manager/ut/common
    yql/essentials/sql/pg_dummy
    library/cpp/json
)

YQL_LAST_ABI_VERSION()

END()
