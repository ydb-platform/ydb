PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(13)
SIZE(MEDIUM)


<<<<<<< HEAD
=======
REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()
>>>>>>> 7bf789f021c (Main: Optimisation for medium and small tests cpu requirments (without split and fork) (#35835))

TEST_SRCS(
    test_copy_table.py
)

PEERDIR(
    ydb/tests/sql/lib
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
