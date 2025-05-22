UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(200)

IF (WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
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
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/digest
)


DATA (
    arcadia/ydb/core/kqp/ut/join
)

YQL_LAST_ABI_VERSION()

END()
