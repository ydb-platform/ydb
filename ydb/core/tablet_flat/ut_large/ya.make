UNITTEST_FOR(ydb/core/tablet_flat)

IF (WITH_VALGRIND)
    TAG(ya:fat)
    SIZE(LARGE)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    flat_executor_ut_large.cpp
    ut_btree_index_large.cpp
)

PEERDIR(
    ydb/core/scheme
    ydb/core/tablet_flat/test/libs/exec
    ydb/core/tablet_flat/test/libs/table
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

END()
