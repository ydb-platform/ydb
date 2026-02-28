# platformdirs

[![PyPI version](https://badge.fury.io/py/platformdirs.svg)](https://badge.fury.io/py/platformdirs)
[![Python versions](https://img.shields.io/pypi/pyversions/platformdirs.svg)](https://pypi.python.org/pypi/platformdirs/)
[![CI](https://github.com/tox-dev/platformdirs/actions/workflows/check.yaml/badge.svg)](https://github.com/platformdirs/platformdirs/actions)
[![Downloads](https://static.pepy.tech/badge/platformdirs/month)](https://pepy.tech/project/platformdirs)

A Python package for determining platform-specific directories (e.g. user data, config, cache, logs). Handles the
differences between macOS, Windows, Linux/Unix, and Android so you don't have to.

## Quick start

```python
from platformdirs import PlatformDirs

dirs = PlatformDirs("MyApp", "MyCompany")
dirs.user_data_dir  # e.g. ~/.local/share/MyApp on Linux
dirs.user_config_dir  # e.g. ~/.config/MyApp on Linux
dirs.user_cache_dir  # e.g. ~/.cache/MyApp on Linux
dirs.user_log_dir  # e.g. ~/.local/state/MyApp/log on Linux
```

Convenience functions are also available:

```python
from platformdirs import user_data_dir, user_config_path

user_data_dir("MyApp", "MyCompany")  # returns str
user_config_path("MyApp", "MyCompany")  # returns pathlib.Path
```

## Documentation

Full documentation is available at [platformdirs.readthedocs.io](https://platformdirs.readthedocs.io), including:

- [Usage guide](https://platformdirs.readthedocs.io/en/latest/usage.html) -- parameters, examples, and patterns
- [API reference](https://platformdirs.readthedocs.io/en/latest/api.html) -- all functions and classes
- [Platform details](https://platformdirs.readthedocs.io/en/latest/platforms.html) -- per-platform paths and behavior

## Why this fork?

This repository is a friendly fork of the wonderful work started by
[ActiveState](https://github.com/ActiveState/appdirs) who created `appdirs`, this package's ancestor.

Maintaining an open source project is no easy task, particularly from within an organization, and the Python community
is indebted to `appdirs` (and to Trent Mick and Jeff Rouse in particular) for creating an incredibly useful simple
module, as evidenced by the wide number of users it has attracted over the years.

Nonetheless, given the number of long-standing open issues and pull requests, and no clear path towards
[ensuring that maintenance of the package would continue or grow](https://github.com/ActiveState/appdirs/issues/79),
this fork was created.

Contributions are most welcome.
