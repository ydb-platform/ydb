PY3TEST()

TEST_SRCS(
    test_bootstrap.py
    test_resource_path_prefix_mapping.py
    test_state_storage.py
)

PEERDIR(
    contrib/python/pytest
    ydb/tools/cfg
)

END()
