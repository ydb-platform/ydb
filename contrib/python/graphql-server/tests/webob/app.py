from urllib.parse import urlencode

from webob import Request

from graphql_server.webob import GraphQLView

from .schema import Schema


def url_string(url="/graphql", **url_params):
    return f"{url}?{urlencode(url_params)}" if url_params else url


class Client(object):
    def __init__(self, **kwargs):
        self.schema = kwargs.pop("schema", None) or Schema
        self.settings = kwargs.pop("settings", None) or {}

    def get(self, url, **extra):
        request = Request.blank(url, method="GET", **extra)
        context = self.settings.pop("context", request)
        response = GraphQLView(
            request=request, schema=self.schema, context=context, **self.settings
        )
        return response.dispatch_request(request)

    def post(self, url, **extra):
        extra["POST"] = extra.pop("data")
        request = Request.blank(url, method="POST", **extra)
        context = self.settings.pop("context", request)
        response = GraphQLView(
            request=request, schema=self.schema, context=context, **self.settings
        )
        return response.dispatch_request(request)

    def put(self, url, **extra):
        request = Request.blank(url, method="PUT", **extra)
        context = self.settings.pop("context", request)
        response = GraphQLView(
            request=request, schema=self.schema, context=context, **self.settings
        )
        return response.dispatch_request(request)
