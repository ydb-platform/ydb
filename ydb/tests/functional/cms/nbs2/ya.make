PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_cms_nbs2.py
)

ENV(YDB_DSTOOL_BINARY="ydb/apps/dstool/ydb-dstool")

SIZE(LARGE)
TIMEOUT(600)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
REQUIREMENTS(cpu:4)
REQUIREMENTS(ram:16)

DEPENDS(
    ydb/apps/dstool
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos
    ydb/public/api/grpc/draft
    ydb/tests/functional/nbs/lib
    ydb/tests/library
)

END()
