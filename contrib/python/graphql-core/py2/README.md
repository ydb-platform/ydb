# GraphQL-core 2

⚠️ This is the repository of GraphQL for Python 2 (legacy version).

**The repository for the current version is available at
[github.com/graphql-python/graphql-core](https://github.com/graphql-python/graphql-core).**

[![Build Status](https://travis-ci.org/graphql-python/graphql-core-legacy.svg?branch=master)](https://travis-ci.org/graphql-python/graphql-core-legacy)
[![Coverage Status](https://coveralls.io/repos/github/graphql-python/graphql-core-legacy/badge.svg?branch=master)](https://coveralls.io/github/graphql-python/graphql-core-legacy?branch=master)

This library is a port of [GraphQL.js](https://github.com/graphql/graphql-js) to Python
and up-to-date with release [0.6.0](https://github.com/graphql/graphql-js/releases/tag/v0.6.0).

GraphQL-core 2 supports Python version 2.7, 3.5, 3.6, 3.7 and 3.8. 

GraphQL.js is the JavaScript reference implementation for GraphQL,
a query language for APIs created by Facebook.

See also the GraphQL documentation at [graphql.org](https://graphql.org/) and
[graphql.org/graphql-js/graphql/](https://graphql.org/graphql-js/graphql/).

For questions regarding GraphQL, ask [Stack Overflow](http://stackoverflow.com/questions/tagged/graphql).

## Getting Started

An overview of the GraphQL language is available in the
[README](https://github.com/graphql/graphql-spec/blob/master/README.md) for the
[Specification for GraphQL](https://github.com/graphql/graphql-spec).

The overview describes a simple set of GraphQL examples that exist as
[tests](https://github.com/graphql-python/graphql-core-legacy/tree/master/tests/)
in this repository. A good way to get started is to walk through that README
and the corresponding tests in parallel.

### Using GraphQL-core 2

Install from pip:

```sh
pip install "graphql-core<3"
```

GraphQL-core provides two important capabilities: building a type schema, and
serving queries against that type schema.

First, build a GraphQL type schema which maps to your code base.

```python
from graphql import (
    graphql,
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLString
)

schema = GraphQLSchema(
  query=GraphQLObjectType(
    name='RootQueryType',
    fields={
      'hello': GraphQLField(
        type=GraphQLString,
        resolver=lambda *_: 'world'
      )
    }
  )
)
```

This defines a simple schema with one type and one field, that resolves to a fixed value.
The `resolver` function can return a value, a promise, or an array of promises.
A more complex example is included in the top level
[tests](https://github.com/graphql-python/graphql-core-legacy/tree/master/tests/) directory.

Then, serve the result of a query against that type schema.

```python
query = '{ hello }'

result = graphql(schema, query)

# Prints
# {'hello': 'world'} (as OrderedDict)

print result.data
```

This runs a query fetching the one field defined. The `graphql` function will first ensure
the query is syntactically and semantically valid before executing it, reporting errors otherwise.

```python
query = '{ boyhowdy }'

result = graphql(schema, query)

# Prints
# [GraphQLError('Cannot query field "boyhowdy" on type "RootQueryType".',)]

print result.errors
```

### Executors

The graphql query is executed, by default, synchronously (using `SyncExecutor`). However the following executors are available if we want to resolve our fields in parallel:

- `graphql.execution.executors.asyncio.AsyncioExecutor`: This executor executes the resolvers in the Python asyncio event loop.
- `graphql.execution.executors.gevent.GeventExecutor`: This executor executes the resolvers in the Gevent event loop.
- `graphql.execution.executors.process.ProcessExecutor`: This executor executes each resolver as a process.
- `graphql.execution.executors.thread.ThreadExecutor`: This executor executes each resolver in a Thread.
- `graphql.execution.executors.sync.SyncExecutor`: This executor executes each resolver synchronusly (default).

#### Usage

You can specify the executor to use via the executor keyword argument in the `grapqhl.execution.execute` function.

```python
from graphql import parse
from graphql.execution import execute
from graphql.execution.executors.sync import SyncExecutor

ast = parse('{ hello }')

result = execute(schema, ast, executor=SyncExecutor())

print result.data
```

### Contributing

After cloning this repo, create a [virtualenv](https://virtualenv.pypa.io/en/stable/) and ensure dependencies are installed by running:

```sh
virtualenv venv
source venv/bin/activate
pip install -e ".[test]"
```

Well-written tests and maintaining good test coverage is important to this project. While developing, run new and existing tests with:

```sh
pytest PATH/TO/MY/DIR/test_test.py # Single file
pytest PATH/TO/MY/DIR/ # All tests in directory
```

Add the `-s` flag if you have introduced breakpoints into the code for debugging.
Add the `-v` ("verbose") flag to get more detailed test output. For even more detailed output, use `-vv`.
Check out the [pytest documentation](https://docs.pytest.org/en/latest/) for more options and test running controls.

GraphQL-core 2 supports several versions of Python. To make sure that changes do not break compatibility
with any of those versions, we use `tox` to create virtualenvs for each Python version and run tests with that version.
To run against all python versions defined in the `tox.ini` config file, just run:

```sh
tox
```

If you wish to run against a specific version defined in the `tox.ini` file:

```sh
tox -e py36
```

Tox can only use whatever versions of python are installed on your system. When you create a pull request, Travis will also be running the same tests and report the results, so there is no need for potential contributors to try to install every single version of python on their own system ahead of time. We appreciate opening issues and pull requests to make GraphQL-core even more stable & useful!

## Main Contributors

- [@syrusakbary](https://github.com/syrusakbary/)
- [@jhgg](https://github.com/jhgg/)
- [@dittos](https://github.com/dittos/)

## License

[MIT License](https://github.com/graphql-python/graphql-core-legacy/blob/master/LICENSE)
