PY3TEST()

TEST_SRCS(
    test_local_ydb.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

DEPENDS(
    ydb/public/tools/local_ydb
)

ENV(LOCAL_YDB_BINARY="ydb/public/tools/local_ydb/local_ydb")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_KAFKA_PROXY_PORT=0)
ENV(YDB_TINY_MODE=true)

SIZE(MEDIUM)
TIMEOUT(600)

# Do not enable FORK_SUBTESTS: every scenario starts a real ydbd and must run sequentially.
END()
