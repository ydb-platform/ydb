PY3TEST()

FORK_TEST_FILES()
SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

ENV(MVP_META_BINARY="ydb/mvp/meta/bin/mvp_meta")

TEST_SRCS(
    conftest.py
    support_links_env.py
    test_support_links.py
)

DEPENDS(
    ydb/mvp/meta/bin
)

PEERDIR(
    contrib/python/requests
    ydb/tests/functional/mvp/common
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/oss/ydb_sdk_import
)

END()
