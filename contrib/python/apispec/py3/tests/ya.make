PY3TEST()

PEERDIR(
    contrib/python/mock
    contrib/python/Flask
    contrib/python/marshmallow
    contrib/python/PyYAML
    contrib/python/apispec
    contrib/python/prance
    contrib/python/openapi-spec-validator
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_core.py
    test_ext_flask.py
    test_ext_marshmallow.py
    test_ext_marshmallow_common.py
    test_ext_marshmallow_field.py
    test_ext_marshmallow_openapi.py
    test_utils.py
    test_yaml_utils.py
)

PY_SRCS(
    NAMESPACE tests
    plugins/__init__.py
    plugins/dummy_plugin.py
    plugins/dummy_plugin_no_setup.py
    schemas.py
    utils.py
)

NO_LINT()

END()
