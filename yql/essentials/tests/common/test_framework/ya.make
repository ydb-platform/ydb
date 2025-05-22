PY23_LIBRARY()

PY_SRCS(
    TOP_LEVEL
    solomon_runner.py
    yql_utils.py
    yql_ports.py
    yqlrun.py
    yql_http_file_server.py
    test_utils.py
    test_file_common.py
)

PY_SRCS(
    NAMESPACE ydb_library_yql_test_framework
    conftest.py
)

PEERDIR(
    contrib/python/requests
    contrib/python/six
    contrib/python/urllib3
    library/python/cyson
    yql/essentials/core/file_storage/proto
    yql/essentials/providers/common/proto
)

END()

RECURSE(
    udfs_deps
)
