PY3TEST()
TEST_SRCS(
    test_duplicates.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/tpc/medium/tpch/common.inc)
REQUIREMENTS(cpu:2)

# Share class setup and imported data across all 22 queries.
END()
