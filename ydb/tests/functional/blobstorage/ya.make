PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
TEST_SRCS(
    test_pdisk_format_info.py
    test_replication.py
    test_self_heal.py
    test_tablet_channel_migration.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SPLIT_FACTOR(20)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
    contrib/python/PyHamcrest
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
