PY3TEST()

PEERDIR(
    contrib/python/argcomplete
    contrib/python/traitlets
    contrib/python/pytest-mock
)

TEST_SRCS(
    __init__.py
    _warnings.py
    config/__init__.py
    config/test_application.py
    config/test_argcomplete.py
    config/test_configurable.py
    config/test_loader.py
    test_traitlets.py
    test_traitlets_docstring.py
    test_traitlets_enum.py
    test_typing.py
    utils/__init__.py
    utils/test_bunch.py
    utils/test_decorators.py
    utils/test_importstring.py
)

NO_LINT()

END()
