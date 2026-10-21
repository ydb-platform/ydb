PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()

SIZE(MEDIUM)

TEST_SRCS(
    test_column_family_negative.py
    test_disabled.py
    test_incorrect.py
    test_mixed.py
)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    ydb/tests/olap/scenario/helpers
    ydb/tests/olap/common
)

DEPENDS(
)

END()
