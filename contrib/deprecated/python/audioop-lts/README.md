# audioop

An LTS port of the Python builtin module `audioop` which was deprecated since version 3.11 and removed in 3.13.

This project exists to maintain this module for future versions.

## Using this project

> [!WARNING]
**This module only functions at Python versions of 3.13 or greater** due to being removed in this version.

As such, you can conditionally add this project to your dependencies:

#### pip-requirements
```
audioop-lts; python_version>='3.13'
```

#### Poetry-pyproject
```toml
[tool.poetry.dependencies]
audioop-lts = { version = "...", python = "^3.13" }
```
Relevant documentation is [here](https://python-poetry.org/docs/dependency-specification/#python-restricted-dependencies), or alternatively use [`markers`](https://python-poetry.org/docs/dependency-specification/#using-environment-markers)

#### pdm-pyproject / uv-pyproject / hatch-pyproject
```toml
[project]
dependencies = [
    "audioop-lts; python_version >= '3.13'",
]
```

#### Pipenv-pipfile
```toml
[packages]
audioop-lts = { version = "...", markers = "python_version >= 3.13" }
```
