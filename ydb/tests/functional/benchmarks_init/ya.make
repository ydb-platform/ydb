PY3TEST()

TEST_SRCS(
    test_generator.py
    test_init.py
)

<<<<<<< HEAD
SIZE(MEDIUM)

IF(NOT SANITIZER_TYPE)
    REQUIREMENTS(ram:8)
ELSE()
    REQUIREMENTS(ram:16)
=======
REQUIREMENTS(ram:16 cpu:4)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
>>>>>>> 7bf789f021c (Main: Optimisation for medium and small tests cpu requirments (without split and fork) (#35835))
ENDIF()

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
    contrib/python/PyHamcrest
    ydb/tests/library
)

FORK_SUBTESTS()
FORK_TEST_FILES()
END()
