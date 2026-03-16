# GQL

This is a GraphQL client for Python 3.7+.
Plays nicely with `graphene`, `graphql-core`, `graphql-js` and any other GraphQL implementation compatible with the spec.

GQL architecture is inspired by `React-Relay` and `Apollo-Client`.

[![GitHub-Actions][gh-image]][gh-url]
[![pyversion][pyversion-image]][pyversion-url]
[![pypi][pypi-image]][pypi-url]
[![Anaconda-Server Badge][conda-image]][conda-url]
[![codecov][codecov-image]][codecov-url]

[gh-image]: https://github.com/graphql-python/gql/workflows/Tests/badge.svg
[gh-url]: https://github.com/graphql-python/gql/actions?query=workflow%3ATests
[pyversion-image]: https://img.shields.io/pypi/pyversions/gql
[pyversion-url]: https://pypi.org/project/gql/
[pypi-image]: https://img.shields.io/pypi/v/gql.svg?style=flat
[pypi-url]: https://pypi.org/project/gql/
[conda-image]: https://img.shields.io/conda/vn/conda-forge/gql.svg
[conda-url]: https://anaconda.org/conda-forge/gql
[codecov-image]: https://codecov.io/gh/graphql-python/gql/branch/master/graph/badge.svg
[codecov-url]: https://codecov.io/gh/graphql-python/gql

## Documentation

The complete documentation for GQL can be found at
[gql.readthedocs.io](https://gql.readthedocs.io).

## Features

* Execute GraphQL queries using [different protocols](https://gql.readthedocs.io/en/latest/transports/index.html):
  * http
  * websockets:
    * apollo or graphql-ws protocol
    * Phoenix channels
    * AWS AppSync realtime protocol (experimental)
* Possibility to [validate the queries locally](https://gql.readthedocs.io/en/latest/usage/validation.html) using a GraphQL schema provided locally or fetched from the backend using an instrospection query
* Supports GraphQL queries, mutations and [subscriptions](https://gql.readthedocs.io/en/latest/usage/subscriptions.html)
* Supports [sync or async usage](https://gql.readthedocs.io/en/latest/async/index.html), [allowing concurrent requests](https://gql.readthedocs.io/en/latest/advanced/async_advanced_usage.html#async-advanced-usage)
* Supports [File uploads](https://gql.readthedocs.io/en/latest/usage/file_upload.html)
* Supports [Custom scalars / Enums](https://gql.readthedocs.io/en/latest/usage/custom_scalars_and_enums.html)
* [gql-cli script](https://gql.readthedocs.io/en/latest/gql-cli/intro.html) to execute GraphQL queries or download schemas from the command line
* [DSL module](https://gql.readthedocs.io/en/latest/advanced/dsl_module.html) to compose GraphQL queries dynamically

## Installation

You can install GQL with all the optional dependencies using pip:

```bash
# Quotes may be required on certain shells such as zsh.
pip install "gql[all]"
```

> **NOTE**: See also [the documentation](https://gql.readthedocs.io/en/latest/intro.html#less-dependencies) to install GQL with less extra dependencies depending on the transports you would like to use or for alternative installation methods.

## Usage

### Basic usage

```python
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

# Select your transport with a defined url endpoint
transport = AIOHTTPTransport(url="https://countries.trevorblades.com/")

# Create a GraphQL client using the defined transport
client = Client(transport=transport, fetch_schema_from_transport=True)

# Provide a GraphQL query
query = gql(
    """
    query getContinents {
      continents {
        code
        name
      }
    }
"""
)

# Execute the query on the transport
result = client.execute(query)
print(result)
```

Executing the above code should output the following result:

```
$ python basic_example.py
{'continents': [{'code': 'AF', 'name': 'Africa'}, {'code': 'AN', 'name': 'Antarctica'}, {'code': 'AS', 'name': 'Asia'}, {'code': 'EU', 'name': 'Europe'}, {'code': 'NA', 'name': 'North America'}, {'code': 'OC', 'name': 'Oceania'}, {'code': 'SA', 'name': 'South America'}]}
```

> **WARNING**: Please note that this basic example won't work if you have an asyncio event loop running. In some
> python environments (as with Jupyter which uses IPython) an asyncio event loop is created for you. In that case you
> should use instead the [async usage example](https://gql.readthedocs.io/en/latest/async/async_usage.html#async-usage).

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md)

## License

[MIT License](https://github.com/graphql-python/gql/blob/master/LICENSE)
