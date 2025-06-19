PY3TEST()

TEST_SRCS(
    test_config_with_metadata.py
    test_generate_dynamic_config.py
)

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()


INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
