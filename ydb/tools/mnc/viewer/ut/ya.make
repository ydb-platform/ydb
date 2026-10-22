PY3TEST()

SIZE(MEDIUM)
TIMEOUT(120)
FORK_SUBTESTS()
SPLIT_FACTOR(8)

TEST_SRCS(
    test_cluster_config_validation.py
    test_navigation.py
)

PEERDIR(
    ydb/tools/mnc/viewer
)

END()
