PY2TEST()

PEERDIR(
    contrib/python/jupyter-core
    contrib/python/mock
)

DATA(
    arcadia/contrib/python/jupyter-core/py2/jupyter_core/tests/dotipython/
    arcadia/contrib/python/jupyter-core/py2/jupyter_core/tests/dotipython_empty/
)

DEPENDS(
    contrib/python/jupyter-core/py2/bin
)

SRCDIR(contrib/python/jupyter-core/py2/jupyter_core/tests)

TEST_SRCS(
    __init__.py
    mocking.py
    test_application.py
    test_migrate.py
    test_paths.py
    test_command.py
)

NO_LINT()

END()
