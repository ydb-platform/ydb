PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    yql/essentials/providers/common/proto
)

TEST_SRCS(
    kikimr_config.py
    test_bs_config_invoke.py
)

END()
