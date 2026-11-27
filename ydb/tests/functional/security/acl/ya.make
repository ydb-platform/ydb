PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    test_anonymous_access.py
    test_grants.py
    test_paths_lookup.py
)

FORK_SUBTESTS()
SPLIT_FACTOR(10)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/library/flavours/flavours_deps.inc)

DEPENDS(
)

PEERDIR(
    ydb/tests/functional/security/lib
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/library/flavours
    ydb/tests/oss/ydb_sdk_import
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
