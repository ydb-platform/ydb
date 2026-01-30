PY3TEST()

PEERDIR(
    contrib/python/typeguard
    contrib/python/typing-extensions
)

TEST_SRCS(
    conftest.py
    __init__.py
    mypy/test_type_annotations.py
    pep695.py
    test_checkers.py
    test_importhook.py
    test_instrumentation.py
    test_plugins.py
    test_pytest_plugin.py
    test_suppression.py
    test_transformer.py
    test_typechecked.py
    test_utils.py
    test_warn_on_error.py
)

DATA(
    arcadia/contrib/python/typeguard/tests
)

NO_LINT()

END()
