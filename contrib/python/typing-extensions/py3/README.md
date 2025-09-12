# Typing Extensions

[![Chat at https://gitter.im/python/typing](https://badges.gitter.im/python/typing.svg)](https://gitter.im/python/typing)

[Documentation](https://typing-extensions.readthedocs.io/en/latest/#) –
[PyPI](https://pypi.org/project/typing-extensions/)

## Overview

The `typing_extensions` module serves two related purposes:

- Enable use of new type system features on older Python versions. For example,
  `typing.TypeGuard` is new in Python 3.10, but `typing_extensions` allows
  users on previous Python versions to use it too.
- Enable experimentation with new type system PEPs before they are accepted and
  added to the `typing` module.

`typing_extensions` is treated specially by static type checkers such as
mypy and pyright. Objects defined in `typing_extensions` are treated the same
way as equivalent forms in `typing`.

`typing_extensions` uses
[Semantic Versioning](https://semver.org/). The
major version will be incremented only for backwards-incompatible changes.
Therefore, it's safe to depend
on `typing_extensions` like this: `typing_extensions ~=x.y`,
where `x.y` is the first version that includes all features you need.
[This](https://packaging.python.org/en/latest/specifications/version-specifiers/#compatible-release)
is equivalent to `typing_extensions >=x.y, <(x+1)`. Do not depend on `~= x.y.z`
unless you really know what you're doing; that defeats the purpose of
semantic versioning.

## Included items

See [the documentation](https://typing-extensions.readthedocs.io/en/latest/#) for a
complete listing of module contents.

## Contributing

See [CONTRIBUTING.md](https://github.com/python/typing_extensions/blob/main/CONTRIBUTING.md)
for how to contribute to `typing_extensions`.
