PY3TEST()

TEST_SRCS(
    test_agent_client.py
    test_disks.py
    test_main.py
)

PY_SRCS(
    helpers.py
)

PEERDIR(
    ydb/tools/mnc/cli
    ydb/tools/mnc/lib
)

END()
