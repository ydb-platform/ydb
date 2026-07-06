PY3TEST()

FORK_TEST_FILES()
SPLIT_FACTOR(2)

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
