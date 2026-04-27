PY3_LIBRARY()

PY_SRCS(
    __init__.py
    secrets_plugin.py
)

PEERDIR(
    contrib/python/pytest
    ydb/tests/library/fixtures
    ydb/tests/library/flavours
    ydb/tests/oss/ydb_sdk_import
)

END()
