PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

ENV(YDB_DSTOOL_BINARY="ydb/apps/dstool/ydb-dstool")

PY_SRCS (
    common.py
    helpers.py
    vhost_user_blk_client.py
)
TEST_SRCS(
    test_nbs.py
    test_nbs_load_actor.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

REQUIREMENTS(ram:16)

DEPENDS(
    ydb/apps/dstool
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
)

END()

RECURSE_FOR_TESTS(
    F1_user_scenarios
    F1_vhost
    F2_fault_injection
    F3_node_down_and_data_copy
    F4_throttling_and_limits
    F5_observability
)
