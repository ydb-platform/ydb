import pytest
from flask import url_for
from jinja2 import Environment

from .app import create_app


@pytest.mark.parametrize(
    "app",
    [create_app(graphiql=True), create_app(graphiql=True, jinja_env=Environment())],
)
def test_graphiql_is_enabled(app, client):
    with app.test_request_context():
        response = client.get(
            url_for("graphql", externals=False), headers={"Accept": "text/html"}
        )
    assert response.status_code == 200


@pytest.mark.parametrize(
    "app",
    [create_app(graphiql=True), create_app(graphiql=True, jinja_env=Environment())],
)
def test_graphiql_renders_pretty(app, client):
    with app.test_request_context():
        response = client.get(
            url_for("graphql", query="{test}"), headers={"Accept": "text/html"}
        )
    assert response.status_code == 200
    pretty_response = (
        "{\n"
        '  "data": {\n'
        '    "test": "Hello World"\n'
        "  }\n"
        "}".replace('"', '\\"').replace("\n", "\\n")
    )

    assert pretty_response in response.data.decode("utf-8")


@pytest.mark.parametrize(
    "app",
    [create_app(graphiql=True), create_app(graphiql=True, jinja_env=Environment())],
)
def test_graphiql_default_title(app, client):
    with app.test_request_context():
        response = client.get(url_for("graphql"), headers={"Accept": "text/html"})
    assert "<title>GraphiQL</title>" in response.data.decode("utf-8")


@pytest.mark.parametrize(
    "app",
    [
        create_app(graphiql=True, graphiql_html_title="Awesome"),
        create_app(
            graphiql=True, graphiql_html_title="Awesome", jinja_env=Environment()
        ),
    ],
)
def test_graphiql_custom_title(app, client):
    with app.test_request_context():
        response = client.get(url_for("graphql"), headers={"Accept": "text/html"})
    assert "<title>Awesome</title>" in response.data.decode("utf-8")
