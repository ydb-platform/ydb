PY3TEST()

TEST_SRCS(
    test_agent.py
    test_agent_client.py
    test_command_options.py
    test_configs.py
    test_disks.py
    test_install.py
    test_main.py
    test_nbs.py
    test_output.py
    test_progress.py
    test_qemu.py
    test_service.py
    test_uninstall.py
    test_verbose.py
    test_tui.py
)

PY_SRCS(
    helpers.py
)

PEERDIR(
    ydb/tools/mnc/cli
    ydb/tools/mnc/lib
)

END()
