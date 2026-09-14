PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ELSE()
    REQUIREMENTS(cpu:4)
ENDIF()

TEST_SRCS(
    conftest.py
    test_canonical_requests.py
    test_static_vdisk_evict.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/apps/dstool
    ydb/apps/dstool/lib
)

END()
