from functools import partial

from flask import Response, request
from flask.views import View

from graphql.type.schema import GraphQLSchema
from graphql_server import (HttpQueryError, default_format_error,
                            encode_execution_results, json_encode,
                            load_json_body, run_http_query)

from .render_graphiql import render_graphiql


class GraphQLView(View):
    schema = None
    executor = None
    root_value = None
    pretty = False
    graphiql = False
    backend = None
    graphiql_version = None
    graphiql_template = None
    graphiql_html_title = None
    middleware = None
    batch = False

    methods = ['GET', 'POST', 'PUT', 'DELETE']

    def __init__(self, **kwargs):
        super(GraphQLView, self).__init__()
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

        assert isinstance(self.schema, GraphQLSchema), 'A Schema is required to be provided to GraphQLView.'

    # noinspection PyUnusedLocal
    def get_root_value(self):
        return self.root_value

    def get_context(self):
        return request

    def get_middleware(self):
        return self.middleware

    def get_backend(self):
        return self.backend

    def get_executor(self):
        return self.executor

    def render_graphiql(self, params, result):
        return render_graphiql(
            params=params,
            result=result,
            graphiql_version=self.graphiql_version,
            graphiql_template=self.graphiql_template,
            graphiql_html_title=self.graphiql_html_title,
        )

    format_error = staticmethod(default_format_error)
    encode = staticmethod(json_encode)

    def dispatch_request(self):
        try:
            request_method = request.method.lower()
            data = self.parse_body()

            show_graphiql = request_method == 'get' and self.should_display_graphiql()
            catch = show_graphiql

            pretty = self.pretty or show_graphiql or request.args.get('pretty')

            extra_options = {}
            executor = self.get_executor()
            if executor:
                # We only include it optionally since
                # executor is not a valid argument in all backends
                extra_options['executor'] = executor

            execution_results, all_params = run_http_query(
                self.schema,
                request_method,
                data,
                query_data=request.args,
                batch_enabled=self.batch,
                catch=catch,
                backend=self.get_backend(),

                # Execute options
                root=self.get_root_value(),
                context=self.get_context(),
                middleware=self.get_middleware(),
                **extra_options
            )
            result, status_code = encode_execution_results(
                execution_results,
                is_batch=isinstance(data, list),
                format_error=self.format_error,
                encode=partial(self.encode, pretty=pretty)
            )

            if show_graphiql:
                return self.render_graphiql(
                    params=all_params[0],
                    result=result
                )

            return Response(
                result,
                status=status_code,
                content_type='application/json'
            )

        except HttpQueryError as e:
            return Response(
                self.encode({
                    'errors': [self.format_error(e)]
                }),
                status=e.status_code,
                headers=e.headers,
                content_type='application/json'
            )

    # Flask
    # noinspection PyBroadException
    def parse_body(self):
        # We use mimetype here since we don't need the other
        # information provided by content_type
        content_type = request.mimetype
        if content_type == 'application/graphql':
            return {'query': request.data.decode('utf8')}

        elif content_type == 'application/json':
            return load_json_body(request.data.decode('utf8'))

        elif content_type in ('application/x-www-form-urlencoded', 'multipart/form-data'):
            return request.form

        return {}

    def should_display_graphiql(self):
        if not self.graphiql or 'raw' in request.args:
            return False

        return self.request_wants_html()

    def request_wants_html(self):
        best = request.accept_mimetypes \
            .best_match(['application/json', 'text/html'])
        return best == 'text/html' and \
            request.accept_mimetypes[best] > \
            request.accept_mimetypes['application/json']
