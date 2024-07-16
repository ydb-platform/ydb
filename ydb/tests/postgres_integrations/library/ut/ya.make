PY3TEST()

TEST_SRCS(
    integrations_test.py
)


DATA(
    arcadia/ydb/tests/postgres_integrations/library/ut/data
)

PEERDIR(
    ydb/tests/postgres_integrations/library
)

REQUIREMENTS(ram:10)

END()
