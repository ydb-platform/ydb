PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_DSTOOL_BINARY="ydb/apps/dstool/ydb-dstool")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_KV_VOLUME_TOOL_PATH="ydb/tests/stress/kv_volume_tool/kv_volume_tool")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/dstool
    ydb/tests/stress/kv_volume_tool
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/library
    ydb/tests/library/stress
)

END()
