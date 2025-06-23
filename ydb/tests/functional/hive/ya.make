PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
TEST_SRCS(
    hive_matchers.py
    test_create_tablets.py
    test_kill_tablets.py
    test_drain.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:2)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32 cpu:2)
    SPLIT_FACTOR(20)
ELSE()
    SIZE(MEDIUM)
    SPLIT_FACTOR(20)
ENDIF()

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
