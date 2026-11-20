PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2 ram:8)

TEST_SRCS(
    test_validate_indexes.py
)

DEPENDS(
    ydb/tools/validate_indexes/bin
)

PEERDIR(
    ydb/tests/library
    ydb/tools/validate_indexes
)

END()
