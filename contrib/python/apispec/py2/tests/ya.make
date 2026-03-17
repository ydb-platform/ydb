PY2TEST()

PEERDIR(
    contrib/python/mock
    contrib/python/Flask
    contrib/python/marshmallow
    contrib/python/PyYAML
    contrib/python/apispec
    contrib/python/tornado
)

TEST_SRCS(
    plugins/__init__.py
    plugins/dummy_plugin.py
    plugins/dummy_plugin_no_setup.py
    __init__.py
    conftest.py
    test_core.py
    # test_ext_bottle.py  # bottle is not used by Yandex
    test_ext_flask.py
    test_ext_marshmallow.py
    test_ext_order.py
    test_ext_tornado.py
    test_openapi.py
    test_utils.py
)

PY_SRCS(
    NAMESPACE tests
    schemas.py
)

NO_LINT()

END()
