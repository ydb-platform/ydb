PY23_LIBRARY()

PY_SRCS(
    TOP_LEVEL
    yql_utils.py
    yqlrun.py
)

PEERDIR(
    contrib/python/six
    contrib/python/urllib3
    library/python/cyson
    ydb/library/yql/core/file_storage/proto
    ydb/library/yql/providers/common/proto
)

END()
