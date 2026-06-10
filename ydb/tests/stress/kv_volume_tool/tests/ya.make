PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_KV_VOLUME_TOOL_PATH="ydb/tests/stress/kv_volume_tool/kv_volume_tool")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/kv_volume_tool
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)

END()
