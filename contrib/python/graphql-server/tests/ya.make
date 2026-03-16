PY3TEST()

CONFTEST_LOAD_POLICY_LOCAL()

PEERDIR(
    contrib/python/Flask
    contrib/python/WebOb
    contrib/python/aiohttp
    contrib/python/sanic
    contrib/python/graphql-server
    contrib/python/pytest-asyncio
)

TEST_SRCS(
    aiohttp/__init__.py
    aiohttp/app.py
    aiohttp/conftest.py
    aiohttp/schema.py
    aiohttp/test_graphiqlview.py
    aiohttp/test_graphqlview.py
    flask/__init__.py
    flask/app.py
    flask/conftest.py
    flask/schema.py
    flask/test_graphiqlview.py
    flask/test_graphqlview.py
    webob/__init__.py
    webob/app.py
    webob/conftest.py
    webob/schema.py
    webob/test_graphiqlview.py
    webob/test_graphqlview.py
    __init__.py
    schema.py
    test_asyncio.py
    test_error.py
    test_helpers.py
    test_query.py
    test_version.py
    utils.py
)

NO_LINT()

END()
