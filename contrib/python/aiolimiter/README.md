# aiolimiter

[![Azure Pipelines status for master branch][azure_badge]][azure_status]
[![codecov.io status for master branch][codecov_badge]][codecov_status]
[![Latest PyPI package version][pypi_badge]][aiolimiter_release]
[![Latest Read The Docs][rtd_badge]][aiolimiter_docs]

[azure_badge]: https://dev.azure.com/mjpieters/aiolimiter/_apis/build/status/CI?branchName=master
[azure_status]: https://dev.azure.com/mjpieters/aiolimiter/_build/latest?definitionId=4&branchName=master "Azure Pipelines status for master branch"
[codecov_badge]: https://codecov.io/gh/mjpieters/aiolimiter/branch/master/graph/badge.svg
[codecov_status]: https://codecov.io/gh/mjpieters/aiolimiter "codecov.io status for master branch"
[pypi_badge]: https://badge.fury.io/py/aiolimiter.svg
[aiolimiter_release]: https://pypi.org/project/aiolimiter "Latest PyPI package version"
[rtd_badge]: https://readthedocs.org/projects/aiolimiter/badge/?version=latest
[aiolimiter_docs]: https://aiolimiter.readthedocs.io/en/latest/?badge=latest "Latest Read The Docs"

## Introduction

An efficient implementation of a rate limiter for asyncio.

This project implements the [Leaky bucket algorithm][], giving you precise control over the rate a code section can be entered:

```python
from aiolimiter import AsyncLimiter

# allow for 100 concurrent entries within a 30 second window
rate_limit = AsyncLimiter(100, 30)


async def some_coroutine():
    async with rate_limit:
        # this section is *at most* going to entered 100 times
        # in a 30 second period.
        await do_something()
```

It was first developed [as an answer on Stack Overflow][so45502319].

## Documentation

https://aiolimiter.readthedocs.io

## Installation

```sh
$ pip install aiolimiter
```

The library requires Python 3.8 or newer.

## Requirements

- Python >= 3.8

## License

`aiolimiter` is offered under the [MIT license](./LICENSE.txt).

## Source code

The project is hosted on [GitHub][].

Please file an issue in the [bug tracker][] if you have found a bug
or have some suggestions to improve the library.

## Developer setup

This project uses [poetry][] to manage dependencies, testing and releases. Make sure you have installed that tool, then run the following command to get set up:

```sh
poetry install --with docs && poetry run doit devsetup
```

Apart from using `poetry run doit devsetup`, you can either use `poetry shell` to enter a shell environment with a virtualenv set up for you, or use `poetry run ...` to run commands within the virtualenv.

Tests are run with `pytest` and `tox`. Releases are made with `poetry build` and `poetry publish`. Code quality is maintained with `flake8`, `black` and `mypy`, and `pre-commit` runs quick checks to maintain the standards set.

A series of `doit` tasks are defined; run `poetry run doit list` (or `doit list` with `poetry shell` activated) to list them. The default action is to run a full linting, testing and building run. It is recommended you run this before creating a pull request.

[leaky bucket algorithm]: https://en.wikipedia.org/wiki/Leaky_bucket
[so45502319]: https://stackoverflow.com/a/45502319/100297
[github]: https://github.com/mjpieters/aiolimiter
[bug tracker]: https://github.com/mjpieters/aiolimiter/issues
[poetry]: https://poetry.eustace.io/
