# lxml-stubs
<!-- start-no-pypi -->
[![Python Tests](https://github.com/lxml/lxml-stubs/workflows/Python%20Tests/badge.svg)](https://github.com/lxml/lxml-stubs/actions?query=workflow%3A%22Python+Tests%22)
[![pypi](https://img.shields.io/pypi/v/lxml-stubs.svg)](https://pypi.python.org/pypi/lxml-stubs/)
<!-- end-no-pypi -->

## About
This repository contains external type annotations (see
[PEP 484](https://www.python.org/dev/peps/pep-0484/)) for the
[lxml](http://lxml.de/) package.


## Installation
To use these stubs with [mypy](https://github.com/python/mypy), you have to
install the `lxml-stubs` package.

    pip install lxml-stubs


## Contributing
Contributions should follow the same style guidelines as
[typeshed](https://github.com/python/typeshed/blob/master/CONTRIBUTING.md).


## History
These type annotations were initially included in
[typeshed](https://www.github.com/python/typeshed), but lxml's annotations
are still incomplete and have therefore been extracted from typeshed to
avoid unintentional false positive results.

The code was extracted by Jelle Zijlstra from the original typeshed codebase
and moved to a separate repository using `git filter-branch`.


## Authors
Numerous people have contributed to the lxml stubs; see the git history for
details.
