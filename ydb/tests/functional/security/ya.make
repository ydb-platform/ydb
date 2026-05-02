PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    conftest.py
    test_database_excessive_content_browse_metainfo.py
    test_database_excessive_info_endpoints_contract.py
    test_database_excessive_info_endpoints_auth.py
    test_database_endpoints_invalid_request_returns_400.py
    test_database_requires_database_param_for_viewer_level.py
    test_grants.py
    test_mon_endpoints_auth.py
    test_mon_mtls_auth.py
    test_paths_lookup.py
)

SPLIT_FACTOR(20)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/library/flavours/flavours_deps.inc)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/library/flavours
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:16)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
