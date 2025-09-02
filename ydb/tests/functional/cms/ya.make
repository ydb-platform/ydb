PY3TEST()

PY_SRCS (
    utils.py
)
TEST_SRCS(
    test_cms_erasure.py
    test_cms_restart.py
    test_cms_state_storage.py
)

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()


INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
