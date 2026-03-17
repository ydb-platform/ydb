## Typing stubs for requests

This is a [type stub package](https://typing.python.org/en/latest/tutorials/external_libraries.html)
for the [`requests`](https://github.com/psf/requests) package. It can be used by type checkers
to check code that uses `requests`. This version of
`types-requests` aims to provide accurate annotations for
`requests~=2.32.4`.

Note: `types-requests` has required `urllib3>=2` since v2.31.0.7. If you need to install `types-requests` into an environment that must also have `urllib3<2` installed into it, you will have to use `types-requests<2.31.0.7`.

This package is part of the [typeshed project](https://github.com/python/typeshed).
All fixes for types and metadata should be contributed there.
See [the README](https://github.com/python/typeshed/blob/main/README.md)
for more details. The source for this package can be found in the
[`stubs/requests`](https://github.com/python/typeshed/tree/main/stubs/requests)
directory.

This package was tested with the following type checkers:
* [mypy](https://github.com/python/mypy/) 1.18.2
* [pyright](https://github.com/microsoft/pyright) 1.1.407

It was generated from typeshed commit
[`a68aa254d9e6641f58a7c4f45ab20782226fbfc2`](https://github.com/python/typeshed/commit/a68aa254d9e6641f58a7c4f45ab20782226fbfc2).