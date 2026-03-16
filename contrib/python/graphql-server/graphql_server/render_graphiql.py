"""Based on (express-graphql)[https://github.com/graphql/express-graphql/blob/main/src/renderGraphiQL.ts] and
(graphql-ws)[https://github.com/enisdenjo/graphql-ws]"""
import json
import re
from typing import Any, Dict, Optional, Tuple

# This Environment import is only for type checking purpose,
# and only relevant if rendering GraphiQL with Jinja
try:
    from jinja2 import Environment
except ImportError:  # pragma: no cover
    pass

from typing_extensions import TypedDict

GRAPHIQL_VERSION = "2.2.0"

GRAPHIQL_TEMPLATE = """<!--
The request to this GraphQL server provided the header "Accept: text/html"
and as a result has been presented GraphiQL - an in-browser IDE for
exploring GraphQL.
If you wish to receive JSON, provide the header "Accept: application/json" or
add "&raw" to the end of the URL within a browser.
-->
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>{{graphiql_html_title}}</title>
  <meta name="robots" content="noindex" />
  <meta name="referrer" content="origin" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {
      margin: 0;
      overflow: hidden;
    }
    #graphiql {
      height: 100vh;
    }
  </style>
  <link href="//cdn.jsdelivr.net/npm/graphiql@{{graphiql_version}}/graphiql.css" rel="stylesheet" />
  <script src="//cdn.jsdelivr.net/npm/promise-polyfill@8.2.3/dist/polyfill.min.js"></script>
  <script src="//cdn.jsdelivr.net/npm/unfetch@5.0.0/dist/unfetch.umd.js"></script>
  <script src="//cdn.jsdelivr.net/npm/react@18.2.0/umd/react.production.min.js"></script>
  <script src="//cdn.jsdelivr.net/npm/react-dom@18.2.0/umd/react-dom.production.min.js"></script>
  <script src="//cdn.jsdelivr.net/npm/graphiql@{{graphiql_version}}/graphiql.min.js"></script>
  <script src="//cdn.jsdelivr.net/npm/graphql-ws@5.11.2/umd/graphql-ws.min.js"></script>
</head>
<body>
  <div id="graphiql">Loading...</div>
  <script>
    // Collect the URL parameters
    var parameters = {};
    window.location.search.substr(1).split('&').forEach(function (entry) {
      var eq = entry.indexOf('=');
      if (eq >= 0) {
        parameters[decodeURIComponent(entry.slice(0, eq))] =
          decodeURIComponent(entry.slice(eq + 1));
      }
    });
    // Produce a Location query string from a parameter object.
    function locationQuery(params) {
      return '?' + Object.keys(params).filter(function (key) {
        return Boolean(params[key]);
      }).map(function (key) {
        return encodeURIComponent(key) + '=' +
          encodeURIComponent(params[key]);
      }).join('&');
    }
    // Derive a fetch URL from the current URL, sans the GraphQL parameters.
    var graphqlParamNames = {
      query: true,
      variables: true,
      operationName: true
    };
    var otherParams = {};
    for (var k in parameters) {
      if (parameters.hasOwnProperty(k) && graphqlParamNames[k] !== true) {
        otherParams[k] = parameters[k];
      }
    }
    var fetchURL = locationQuery(otherParams);
    // Defines a GraphQL fetcher.
    var graphQLFetcher;
    if ('{{subscription_url}}') {
      graphQLFetcher = GraphiQL.createFetcher({
        url: fetchURL,
        subscription_url: '{{subscription_url}}'
      });
    } else {
      graphQLFetcher = GraphiQL.createFetcher({ url: fetchURL });
    }
    // When the query and variables string is edited, update the URL bar so
    // that it can be easily shared.
    function onEditQuery(newQuery) {
      parameters.query = newQuery;
      updateURL();
    }
    function onEditVariables(newVariables) {
      parameters.variables = newVariables;
      updateURL();
    }
    function onEditHeaders(newHeaders) {
      parameters.headers = newHeaders;
      updateURL();
    }
    function onEditOperationName(newOperationName) {
      parameters.operationName = newOperationName;
      updateURL();
    }
    function updateURL() {
      history.replaceState(null, null, locationQuery(parameters));
    }
    // Render <GraphiQL /> into the body.
    ReactDOM.render(
      React.createElement(GraphiQL, {
        fetcher: graphQLFetcher,
        onEditQuery: onEditQuery,
        onEditVariables: onEditVariables,
        onEditHeaders: onEditHeaders,
        onEditOperationName: onEditOperationName,
        query: {{query|tojson}},
        response: {{result|tojson}},
        variables: {{variables|tojson}},
        headers: {{headers|tojson}},
        operationName: {{operation_name|tojson}},
        defaultQuery: {{default_query|tojson}},
        isHeadersEditorEnabled: {{header_editor_enabled|tojson}},
        shouldPersistHeaders: {{should_persist_headers|tojson}}
      }),
      document.getElementById('graphiql')
    );
  </script>
</body>
</html>"""


class GraphiQLData(TypedDict):
    """GraphiQL ReactDom Data

    Has the following attributes:

    subscription_url
        The GraphiQL socket endpoint for using subscriptions in graphql-ws.
    headers
        An optional GraphQL string to use as the initial displayed request headers,
        if None is provided, the stored headers will be used.
    """

    query: Optional[str]
    variables: Optional[str]
    operation_name: Optional[str]
    result: Optional[str]
    subscription_url: Optional[str]
    headers: Optional[str]


class GraphiQLConfig(TypedDict):
    """GraphiQL Extra Config

    Has the following attributes:

    graphiql_version
        The version of the provided GraphiQL package.
    graphiql_template
        Inject a Jinja template string to customize GraphiQL.
    graphiql_html_title
        Replace the default html title on the GraphiQL.
    jinja_env
        Sets jinja environment to be used to process GraphiQL template.
        If Jinja’s async mode is enabled (by enable_async=True),
        uses Template.render_async instead of Template.render.
        If environment is not set, fallbacks to simple regex-based renderer.
    """

    graphiql_version: Optional[str]
    graphiql_template: Optional[str]
    graphiql_html_title: Optional[str]
    jinja_env: Optional[Environment]


class GraphiQLOptions(TypedDict):
    """GraphiQL options to display on the UI.

    Has the following attributes:

    default_query
        An optional GraphQL string to use when no query is provided and no stored
        query exists from a previous session. If None is provided, GraphiQL
        will use its own default query.
    header_editor_enabled
        An optional boolean which enables the header editor when true.
        Defaults to false.
    should_persist_headers
        An optional boolean which enables to persist headers to storage when true.
        Defaults to false.
    """

    default_query: Optional[str]
    header_editor_enabled: Optional[bool]
    should_persist_headers: Optional[bool]


def process_var(template: str, name: str, value: Any, jsonify=False) -> str:
    pattern = r"{{\s*" + name.replace("\\", r"\\") + r"(\s*|[^}]+)*\s*}}"
    if jsonify and value not in ["null", "undefined"]:
        value = json.dumps(value)

    value = value.replace("\\", r"\\")

    return re.sub(pattern, value, template)


def simple_renderer(template: str, **values: Dict[str, Any]) -> str:
    replace = [
        "graphiql_version",
        "graphiql_html_title",
        "subscription_url",
        "header_editor_enabled",
        "should_persist_headers",
    ]
    replace_jsonify = [
        "query",
        "result",
        "variables",
        "operation_name",
        "default_query",
        "headers",
    ]

    for r in replace:
        template = process_var(template, r, values.get(r, ""))

    for r in replace_jsonify:
        template = process_var(template, r, values.get(r, ""), True)

    return template


def _render_graphiql(
    data: GraphiQLData,
    config: GraphiQLConfig,
    options: Optional[GraphiQLOptions] = None,
) -> Tuple[str, Dict[str, Any]]:
    """When render_graphiql receives a request which does not Accept JSON, but does
    Accept HTML, it may present GraphiQL, the in-browser GraphQL explorer IDE.
    When shown, it will be pre-populated with the result of having executed
    the requested query.
    """
    graphiql_version = config.get("graphiql_version") or GRAPHIQL_VERSION
    graphiql_template = config.get("graphiql_template") or GRAPHIQL_TEMPLATE
    graphiql_html_title = config.get("graphiql_html_title") or "GraphiQL"

    template_vars: Dict[str, Any] = {
        "graphiql_version": graphiql_version,
        "graphiql_html_title": graphiql_html_title,
        "query": data.get("query"),
        "variables": data.get("variables"),
        "operation_name": data.get("operation_name"),
        "result": data.get("result"),
        "subscription_url": data.get("subscription_url") or "",
        "headers": data.get("headers") or "",
        "default_query": options and options.get("default_query") or "",
        "header_editor_enabled": options
        and options.get("header_editor_enabled")
        or "true",
        "should_persist_headers": options
        and options.get("should_persist_headers")
        or "false",
    }

    if template_vars["result"] in ("null"):
        template_vars["result"] = None

    return graphiql_template, template_vars


async def render_graphiql_async(
    data: GraphiQLData,
    config: GraphiQLConfig,
    options: Optional[GraphiQLOptions] = None,
) -> str:
    graphiql_template, template_vars = _render_graphiql(data, config, options)
    jinja_env = config.get("jinja_env")

    if jinja_env:
        template = jinja_env.from_string(graphiql_template)
        if jinja_env.is_async:
            source = await template.render_async(**template_vars)
        else:
            source = template.render(**template_vars)
    else:
        source = simple_renderer(graphiql_template, **template_vars)
    return source


def render_graphiql_sync(
    data: GraphiQLData,
    config: GraphiQLConfig,
    options: Optional[GraphiQLOptions] = None,
) -> str:
    graphiql_template, template_vars = _render_graphiql(data, config, options)
    jinja_env = config.get("jinja_env")

    if jinja_env:
        template = jinja_env.from_string(graphiql_template)
        source = template.render(**template_vars)
    else:
        source = simple_renderer(graphiql_template, **template_vars)
    return source
