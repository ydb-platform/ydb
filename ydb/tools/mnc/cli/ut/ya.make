PY3TEST()

TEST_SRCS(
    test_agent.py
    test_agent_client.py
    test_disks.py
    test_install.py
    test_main.py
    test_uninstall.py
)

PY_SRCS(
    helpers.py
)

PEERDIR(
    ydb/tools/mnc/cli
    ydb/tools/mnc/lib
)

END()
