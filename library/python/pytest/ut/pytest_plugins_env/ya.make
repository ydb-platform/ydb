PY23_TEST()

TEST_SRCS(
    test_plugin.py
)

PY_SRCS(
    plugin.py
)

PEERDIR(
    library/python/pytest
)

ENV(
    PYTEST_PLUGINS=library.python.pytest.ut.pytest_plugins_env.plugin
)

STYLE_PYTHON()

END()
