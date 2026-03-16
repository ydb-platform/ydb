# GraphQL-Server-Core

[![PyPI version](https://badge.fury.io/py/graphql-server-core.svg)](https://badge.fury.io/py/graphql-server-core)
[![Build Status](https://travis-ci.org/graphql-python/graphql-server-core.svg?branch=master)](https://travis-ci.org/graphql-python/graphql-server-core)
[![Coverage Status](https://codecov.io/gh/graphql-python/graphql-server-core/branch/master/graph/badge.svg)](https://codecov.io/gh/graphql-python/graphql-server-core)

GraphQL-Server-Core is a base library that serves as a helper
for building GraphQL servers or integrations into existing web frameworks using
[GraphQL-Core](https://github.com/graphql-python/graphql-core).

## Existing integrations built with GraphQL-Server-Core

| Server integration | Package |
|---|---|
| Flask | [flask-graphql](https://github.com/graphql-python/flask-graphql/) |
| Sanic |[sanic-graphql](https://github.com/graphql-python/sanic-graphql/) |
| AIOHTTP | [aiohttp-graphql](https://github.com/graphql-python/aiohttp-graphql) |
| WebOb (Pyramid, TurboGears) |  [webob-graphql](https://github.com/graphql-python/webob-graphql/) |
| WSGI | [wsgi-graphql](https://github.com/moritzmhmk/wsgi-graphql) |
| Responder | [responder.ext.graphql](https://github.com/kennethreitz/responder/blob/master/responder/ext/graphql.py) |

## Other integrations using GraphQL-Core or Graphene

| Server integration | Package |
|---|---|
| Django | [graphene-django](https://github.com/graphql-python/graphene-django/) |

## Documentation

The `graphql_server` package provides these public helper functions:

 * `run_http_query`
 * `encode_execution_results`
 * `load_json_body`
 * `json_encode`
 * `json_encode_pretty`

All functions in the package are annotated with type hints and docstrings,
and you can build HTML documentation from these using `bin/build_docs`.

You can also use one of the existing integrations listed above as
blueprint to build your own integration or GraphQL server implementations.

Please let us know when you have built something new, so we can list it here.

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md)
