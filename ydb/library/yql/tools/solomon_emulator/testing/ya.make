PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    solomon_runner.py
)

PY_SRCS(
    NAMESPACE ydb_library_yql_tools_solomon_emulator_testing
    conftest.py
)

PEERDIR(
    contrib/python/requests
)

END()
