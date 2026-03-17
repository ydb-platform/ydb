from typing import Optional

import pytest
from jinja2 import Environment
from quart import Quart, Response, url_for
from quart.typing import TestClientProtocol
from werkzeug.datastructures import Headers

from .app import create_app


@pytest.mark.asyncio
async def execute_client(
    app: Quart,
    client: TestClientProtocol,
    method: str = "GET",
    headers: Optional[Headers] = None,
    **extra_params
) -> Response:
    test_request_context = app.test_request_context(path="/", method=method)
    async with test_request_context:
        string = url_for("graphql", **extra_params)
    return await client.get(string, headers=headers)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "app",
    [create_app(graphiql=True), create_app(graphiql=True, jinja_env=Environment())],
)
async def test_graphiql_is_enabled(app: Quart, client: TestClientProtocol):
    response = await execute_client(
        app, client, headers=Headers({"Accept": "text/html"}), externals=False
    )
    assert response.status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "app",
    [create_app(graphiql=True), create_app(graphiql=True, jinja_env=Environment())],
)
async def test_graphiql_renders_pretty(app: Quart, client: TestClientProtocol):
    response = await execute_client(
        app, client, headers=Headers({"Accept": "text/html"}), query="{test}"
    )
    assert response.status_code == 200
    pretty_response = (
        "{\n"
        '  "data": {\n'
        '    "test": "Hello World"\n'
        "  }\n"
        "}".replace('"', '\\"').replace("\n", "\\n")
    )
    result = await response.get_data(as_text=True)
    assert pretty_response in result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "app",
    [create_app(graphiql=True), create_app(graphiql=True, jinja_env=Environment())],
)
async def test_graphiql_default_title(app: Quart, client: TestClientProtocol):
    response = await execute_client(
        app, client, headers=Headers({"Accept": "text/html"})
    )
    result = await response.get_data(as_text=True)
    assert "<title>GraphiQL</title>" in result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "app",
    [
        create_app(graphiql=True, graphiql_html_title="Awesome"),
        create_app(
            graphiql=True, graphiql_html_title="Awesome", jinja_env=Environment()
        ),
    ],
)
async def test_graphiql_custom_title(app: Quart, client: TestClientProtocol):
    response = await execute_client(
        app, client, headers=Headers({"Accept": "text/html"})
    )
    result = await response.get_data(as_text=True)
    assert "<title>Awesome</title>" in result
