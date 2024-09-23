UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_agg_ut.cpp
    kqp_extract_predicate_unpack_ut.cpp
    kqp_kv_ut.cpp
    kqp_merge_ut.cpp
    kqp_ne_ut.cpp
    kqp_not_null_ut.cpp
    kqp_ranges_ut.cpp
    kqp_sort_ut.cpp
    kqp_sqlin_ut.cpp
    kqp_returning_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/udfs/common/re2
)

ADDINCL(
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

NO_COMPILER_WARNINGS()

YQL_LAST_ABI_VERSION()

END()
