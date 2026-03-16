import pytest
from jinja2 import Environment

from .app import url_string


@pytest.mark.parametrize(
    "settings", [dict(graphiql=True), dict(graphiql=True, jinja_env=Environment())]
)
def test_graphiql_is_enabled(client):
    response = client.get(url_string(query="{test}"), headers={"Accept": "text/html"})
    assert response.status_code == 200


@pytest.mark.parametrize(
    "settings", [dict(graphiql=True), dict(graphiql=True, jinja_env=Environment())]
)
def test_graphiql_simple_renderer(client):
    response = client.get(url_string(query="{test}"), headers={"Accept": "text/html"})
    assert response.status_code == 200
    pretty_response = (
        "{\n"
        '  "data": {\n'
        '    "test": "Hello World"\n'
        "  }\n"
        "}".replace('"', '\\"').replace("\n", "\\n")
    )
    assert pretty_response in response.body.decode("utf-8")


@pytest.mark.parametrize(
    "settings", [dict(graphiql=True), dict(graphiql=True, jinja_env=Environment())]
)
def test_graphiql_html_is_not_accepted(client):
    response = client.get(url_string(), headers={"Accept": "application/json"})
    assert response.status_code == 400
