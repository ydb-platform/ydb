PY3TEST()

ENV(YDB_CHANNEL_BUFFER_SIZE="8388608")

PEERDIR(
    ydb/tests/tools/ydb_serializable/lib
    ydb/public/sdk/python
)

TEST_SRCS(test.py)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)
END()
