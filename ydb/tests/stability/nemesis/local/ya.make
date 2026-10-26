PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_harness_baseline.py
)

SIZE(MEDIUM)

PEERDIR(
    ydb/tests/stability/nemesis
    ydb/tests/library
    ydb/public/sdk/python
)

END()
