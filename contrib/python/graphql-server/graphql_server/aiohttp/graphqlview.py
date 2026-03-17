import asyncio
import copy
from collections.abc import MutableMapping
from functools import partial
from typing import List

from aiohttp import web
from graphql import GraphQLError, specified_rules
from graphql.pyutils import is_awaitable
from graphql.type.schema import GraphQLSchema

from graphql_server import (
    GraphQLParams,
    HttpQueryError,
    _check_jinja,
    encode_execution_results,
    format_error_default,
    json_encode,
    load_json_body,
    run_http_query,
)
from graphql_server.render_graphiql import (
    GraphiQLConfig,
    GraphiQLData,
    GraphiQLOptions,
    render_graphiql_async,
)
from graphql_server.utils import wrap_in_async


class GraphQLView:
    schema = None
    root_value = None
    context = None
    pretty = False
    graphiql = False
    graphiql_version = None
    graphiql_template = None
    graphiql_html_title = None
    middleware = None
    validation_rules = None
    execution_context_class = None
    batch = False
    jinja_env = None
    max_age = 86400
    enable_async = False
    subscriptions = None
    headers = None
    default_query = None
    header_editor_enabled = None
    should_persist_headers = None

    accepted_methods = ["GET", "POST", "PUT", "DELETE"]

    format_error = staticmethod(format_error_default)
    encode = staticmethod(json_encode)

    def __init__(self, **kwargs):
        super(GraphQLView, self).__init__()
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

        if not isinstance(self.schema, GraphQLSchema):
            # maybe the GraphQL schema is wrapped in a Graphene schema
            self.schema = getattr(self.schema, "graphql_schema", None)
            if not isinstance(self.schema, GraphQLSchema):
                raise TypeError("A Schema is required to be provided to GraphQLView.")

        if self.jinja_env is not None:
            _check_jinja(self.jinja_env)

    def get_root_value(self):
        return self.root_value

    def get_context(self, request):
        context = (
            copy.copy(self.context)
            if self.context is not None and isinstance(self.context, MutableMapping)
            else {}
        )
        if isinstance(context, MutableMapping) and "request" not in context:
            context.update({"request": request})
        return context

    def get_middleware(self):
        return self.middleware

    def get_validation_rules(self):
        if self.validation_rules is None:
            return specified_rules
        return self.validation_rules

    def get_execution_context_class(self):
        return self.execution_context_class

    @staticmethod
    async def parse_body(request):
        content_type = request.content_type
        # request.text() is the aiohttp equivalent to
        # request.body.decode("utf8")
        if content_type == "application/graphql":
            r_text = await request.text()
            return {"query": r_text}

        if content_type == "application/json":
            text = await request.text()
            return load_json_body(text)

        if content_type in (
            "application/x-www-form-urlencoded",
            "multipart/form-data",
        ):
            # TODO: seems like a multidict would be more appropriate
            #  than casting it and de-duping variables. Alas, it's what
            #  graphql-python wants.
            return dict(await request.post())

        return {}

    # TODO:
    #  use this method to replace flask and sanic
    #  checks as this is equivalent to `should_display_graphiql` and
    #  `request_wants_html` methods.
    def is_graphiql(self, request):
        return all(
            [
                self.graphiql,
                request.method.lower() == "get",
                "raw" not in request.query,
                any(
                    [
                        "text/html" in request.headers.get("accept", {}),
                        "*/*" in request.headers.get("accept", {}),
                    ]
                ),
            ]
        )

    # TODO: Same stuff as above method.
    def is_pretty(self, request):
        return any(
            [self.pretty, self.is_graphiql(request), request.query.get("pretty")]
        )

    async def __call__(self, request):
        try:
            data = await self.parse_body(request)
            request_method = request.method.lower()
            is_graphiql = self.is_graphiql(request)
            is_pretty = self.is_pretty(request)

            # TODO: way better than if-else so better
            #  implement this too on flask and sanic
            if request_method == "options":
                return self.process_preflight(request)

            all_params: List[GraphQLParams]
            execution_results, all_params = run_http_query(
                self.schema,
                request_method,
                data,
                query_data=request.query,
                batch_enabled=self.batch,
                catch=is_graphiql,
                # Execute options
                run_sync=not self.enable_async,
                root_value=self.get_root_value(),
                context_value=self.get_context(request),
                middleware=self.get_middleware(),
                validation_rules=self.get_validation_rules(),
                execution_context_class=self.get_execution_context_class(),
            )

            exec_res = (
                await asyncio.gather(
                    *(
                        ex
                        if ex is not None and is_awaitable(ex)
                        else wrap_in_async(lambda: ex)()
                        for ex in execution_results
                    )
                )
                if self.enable_async
                else execution_results
            )
            result, status_code = encode_execution_results(
                exec_res,
                is_batch=isinstance(data, list),
                format_error=self.format_error,
                encode=partial(self.encode, pretty=is_pretty),  # noqa: ignore
            )

            if is_graphiql:
                graphiql_data = GraphiQLData(
                    result=result,
                    query=getattr(all_params[0], "query"),
                    variables=getattr(all_params[0], "variables"),
                    operation_name=getattr(all_params[0], "operation_name"),
                    subscription_url=self.subscriptions,
                    headers=self.headers,
                )
                graphiql_config = GraphiQLConfig(
                    graphiql_version=self.graphiql_version,
                    graphiql_template=self.graphiql_template,
                    graphiql_html_title=self.graphiql_html_title,
                    jinja_env=self.jinja_env,
                )
                graphiql_options = GraphiQLOptions(
                    default_query=self.default_query,
                    header_editor_enabled=self.header_editor_enabled,
                    should_persist_headers=self.should_persist_headers,
                )
                source = await render_graphiql_async(
                    data=graphiql_data, config=graphiql_config, options=graphiql_options
                )
                return web.Response(text=source, content_type="text/html")

            return web.Response(
                text=result,
                status=status_code,
                content_type="application/json",
            )

        except HttpQueryError as err:
            parsed_error = GraphQLError(err.message)
            return web.Response(
                body=self.encode(dict(errors=[self.format_error(parsed_error)])),
                status=err.status_code,
                headers=err.headers,
                content_type="application/json",
            )

    def process_preflight(self, request):
        """
        Preflight request support for apollo-client
        https://www.w3.org/TR/cors/#resource-preflight-requests
        """
        headers = request.headers
        origin = headers.get("Origin", "")
        method = headers.get("Access-Control-Request-Method", "").upper()

        if method and method in self.accepted_methods:
            return web.Response(
                status=200,
                headers={
                    "Access-Control-Allow-Origin": origin,
                    "Access-Control-Allow-Methods": ", ".join(self.accepted_methods),
                    "Access-Control-Max-Age": str(self.max_age),
                },
            )
        return web.Response(status=400)

    @classmethod
    def attach(cls, app, *, route_path="/graphql", route_name="graphql", **kwargs):
        view = cls(**kwargs)
        app.router.add_route("*", route_path, _asyncify(view), name=route_name)


def _asyncify(handler):
    """Return an async version of the given handler.

    This is mainly here because ``aiohttp`` can't infer the async definition of
    :py:meth:`.GraphQLView.__call__` and raises a :py:class:`DeprecationWarning`
    in tests. Wrapping it into an async function avoids the noisy warning.
    """

    async def _dispatch(request):
        return await handler(request)

    return _dispatch
