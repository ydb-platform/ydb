PY3TEST()

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

TAG(
    ya:force_sandbox
)

REQUIREMENTS(
    cpu:16
    network:full
    yav:DOCKER_TOKEN_PATH=file:sec-01e0d4agf6pfvwdjwxp61n3fvg:docker_token
)

NO_LINT()

DATA(
    arcadia/contrib/python/umongo
)

PEERDIR(
    contrib/python/umongo
    contrib/python/mongomock
    contrib/python/motor
    contrib/python/Twisted
)

TEST_SRCS(
    __init__.py
    common.py
    conftest.py
    frameworks/__init__.py
    frameworks/common.py
    frameworks/test_mongomock.py
    frameworks/test_motor_asyncio.py
    frameworks/test_pymongo.py
    frameworks/test_txmongo.py
    test_builder.py
    test_data_proxy.py
    test_document.py
    test_embedded_document.py
    test_fields.py
    test_i18n.py
    test_indexes.py
    test_inheritance.py
    test_instance.py
    test_marshmallow.py
    test_query_mapper.py
)

END()
