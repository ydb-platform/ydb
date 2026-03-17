PY3TEST()

PEERDIR(
    contrib/python/jupyter-core
)

DATA(
    arcadia/contrib/python/jupyter-core/py3/tests/dotipython/
    arcadia/contrib/python/jupyter-core/py3/tests/dotipython_empty/
)

DEPENDS(
    contrib/python/jupyter-core/py3/bin
)

TEST_SRCS(
    __init__.py
    mocking.py
    test_application.py
    test_command.py
    test_migrate.py
    test_paths.py
    # test_troubleshoot.py
    test_utils.py
)

NO_LINT()

END()
