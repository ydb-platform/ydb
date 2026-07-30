PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_cluster_workload_config.py
)

PEERDIR(
    contrib/python/PyYAML
    contrib/python/pytest
    ydb/apps/dstool/lib
)

END()
