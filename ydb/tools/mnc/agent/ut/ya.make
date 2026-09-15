PY3TEST()

TEST_SRCS(
    test_api_disks.py
    test_api_nodes.py
    test_api_tasks.py
    test_config.py
    test_database.py
    test_disks.py
    test_features.py
    test_nodes.py
    test_operations.py
    test_tasks.py
)

PEERDIR(
    ydb/tools/mnc/agent
    ydb/tools/mnc/lib
)

END()
