PY3TEST()

PEERDIR(
    contrib/python/aiohttp
    contrib/python/pytest-aiohttp
    contrib/python/pytest-asyncio
    contrib/python/aiohttp-apispec
    contrib/python/marshmallow/py3
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_decorators.py
    test_documentation.py
    test_web_app.py
)

NO_LINT()

END()
