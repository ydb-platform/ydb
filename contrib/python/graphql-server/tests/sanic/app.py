import uuid
from urllib.parse import urlencode

from sanic import Sanic

from graphql_server.sanic import GraphQLView

from .schema import Schema


def create_app(path="/graphql", schema=Schema, **kwargs):
    random_valid_app_name = f"App{uuid.uuid4().hex}"
    app = Sanic(random_valid_app_name)

    app.add_route(GraphQLView.as_view(schema=schema, **kwargs), path)

    return app


def url_string(url="/graphql", **url_params):
    return f"{url}?{urlencode(url_params)}" if url_params else url
