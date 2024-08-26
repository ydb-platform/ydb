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
    kqp_flip_join_ut.cpp
    kqp_index_lookup_join_ut.cpp
    kqp_join_ut.cpp
    kqp_join_order_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/udfs/common/digest
)


DATA (
    arcadia/ydb/core/kqp/ut/join
)

YQL_LAST_ABI_VERSION()

END()
