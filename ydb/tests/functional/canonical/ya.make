PY3TEST()

ENV(YDB_TOKEN="root@builtin")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="GRPC_SERVER:DEBUG,TICKET_PARSER:WARN,KQP_COMPILE_ACTOR:DEBUG")
TEST_SRCS(
    test_sql.py
)

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
DEPENDS(
    ydb/apps/ydbd
)


DATA (
    arcadia/ydb/tests/functional/canonical/canondata
    arcadia/ydb/tests/functional/canonical/sql
)


PEERDIR(
    ydb/tests/library
    ydb/tests/oss/canonical
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
