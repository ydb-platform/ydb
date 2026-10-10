PY3TEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)
TIMEOUT(120)

TEST_SRCS(
    test_cluster_config_validation.py
    test_navigation.py
)

PEERDIR(
    ydb/tools/mnc/viewer
)

END()
