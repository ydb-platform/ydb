from urllib.parse import urlencode

from aiohttp import web

from graphql_server.aiohttp import GraphQLView

from .schema import Schema


def create_app(schema=Schema, **kwargs):
    app = web.Application()
    # Only needed to silence aiohttp deprecation warnings
    GraphQLView.attach(app, schema=schema, **kwargs)
    return app


def url_string(url="/graphql", **url_params):
    return f"{url}?{urlencode(url_params)}" if url_params else url
