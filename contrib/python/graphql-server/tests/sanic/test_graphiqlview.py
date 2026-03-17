import pytest
from jinja2 import Environment

from .app import create_app, url_string
from .schema import AsyncSchema, SyncSchema


@pytest.mark.parametrize(
    "app",
    [
        create_app(graphiql=True),
        create_app(graphiql=True, jinja_env=Environment()),
        create_app(graphiql=True, jinja_env=Environment(enable_async=True)),
    ],
)
def test_graphiql_is_enabled(app):
    _, response = app.test_client.get(
        uri=url_string(query="{test}"), headers={"Accept": "text/html"}
    )

    assert response.status == 200

    pretty_response = (
        "{\n"
        '  "data": {\n'
        '    "test": "Hello World"\n'
        "  }\n"
        "}".replace('"', '\\"').replace("\n", "\\n")
    )

    assert pretty_response in response.body.decode("utf-8")


@pytest.mark.parametrize("app", [create_app(graphiql=True)])
def test_graphiql_html_is_not_accepted(app):
    _, response = app.test_client.get(
        uri=url_string(), headers={"Accept": "application/json"}
    )
    assert response.status == 400


@pytest.mark.parametrize(
    "app", [create_app(schema=AsyncSchema, enable_async=True, graphiql=True)]
)
def test_graphiql_enabled_async_schema(app):
    query = "{a,b,c}"
    _, response = app.test_client.get(
        uri=url_string(query=query), headers={"Accept": "text/html"}
    )

    expected_response = (
        (
            "{\n"
            '  "data": {\n'
            '    "a": "hey",\n'
            '    "b": "hey2",\n'
            '    "c": "hey3"\n'
            "  }\n"
            "}"
        )
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )

    assert response.status == 200
    assert expected_response in response.body.decode("utf-8")


@pytest.mark.parametrize(
    "app", [create_app(schema=SyncSchema, enable_async=True, graphiql=True)]
)
def test_graphiql_enabled_sync_schema(app):
    query = "{a,b}"
    _, response = app.test_client.get(
        uri=url_string(query=query), headers={"Accept": "text/html"}
    )

    expected_response = (
        (
            "{\n"
            '  "data": {\n'
            '    "a": "synced_one",\n'
            '    "b": "synced_two"\n'
            "  }\n"
            "}"
        )
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )
    assert response.status == 200
    assert expected_response in response.body.decode("utf-8")
