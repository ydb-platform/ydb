# elastic-transport-python

[![PyPI](https://img.shields.io/pypi/v/elastic-transport)](https://pypi.org/project/elastic-transport)
[![Python Versions](https://img.shields.io/pypi/pyversions/elastic-transport)](https://pypi.org/project/elastic-transport)
[![PyPI Downloads](https://static.pepy.tech/badge/elastic-transport)](https://pepy.tech/project/elastic-transport)
[![CI Status](https://img.shields.io/github/actions/workflow/status/elastic/elastic-transport-python/ci.yml)](https://github.com/elastic/elastic-transport-python/actions)

Transport classes and utilities shared among Python Elastic client libraries

This library was lifted from [`elasticsearch-py`](https://github.com/elastic/elasticsearch-py)
and then transformed to be used across all Elastic services
rather than only Elasticsearch.

### Installing from PyPI

```
$ python -m pip install elastic-transport
```

Versioning follows the major and minor version of the Elastic Stack version and
the patch number is incremented for bug fixes within a minor release.

## Documentation

Documentation including an API reference is available on [Read the Docs](https://elastic-transport-python.readthedocs.io).

## License

`elastic-transport-python` is available under the Apache-2.0 license.
For more details see [LICENSE](https://github.com/elastic/elastic-transport-python/blob/main/LICENSE).
