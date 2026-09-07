PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_cluster_workload_config.py
    test_cluster_workload_runtime.py
)

PEERDIR(
    contrib/python/PyYAML
    contrib/python/pytest
    ydb/apps/dstool/lib
)

END()
