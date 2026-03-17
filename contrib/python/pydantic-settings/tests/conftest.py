from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


class SetEnv:
    def __init__(self):
        self.envars = set()

    def set(self, name, value):
        self.envars.add(name)
        os.environ[name] = value

    def pop(self, name):
        self.envars.remove(name)
        os.environ.pop(name)

    def clear(self):
        for n in self.envars:
            os.environ.pop(n)


class Dir:
    def __init__(self, basedir: Path) -> None:
        self.basedir = basedir

    def write(self, files: dict[str, str]) -> None:
        for path, content in files.items():
            file_path = self.basedir / path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content)


@pytest.fixture
def tmp_files(tmp_path):
    yield Dir(tmp_path)


@pytest.fixture
def cd_tmp_path(tmp_path: Path) -> Iterator[Path]:
    """Change directory into the value of the ``tmp_path`` fixture.

    .. rubric:: Example
    .. code-block:: python

        from typing import TYPE_CHECKING

        if TYPE_CHECKING:
            from pathlib import Path


        def test_something(cd_tmp_path: Path) -> None:
            ...

    Returns:
        Value of the :fixture:`tmp_path` fixture (a :class:`~pathlib.Path` object).

    """
    prev_dir = Path.cwd()
    os.chdir(tmp_path)
    try:
        yield tmp_path
    finally:
        os.chdir(prev_dir)


@pytest.fixture
def env():
    setenv = SetEnv()

    yield setenv

    setenv.clear()


@pytest.fixture
def docs_test_env():
    setenv = SetEnv()

    # envs for basic usage example
    setenv.set('my_auth_key', 'xxx')
    setenv.set('my_api_key', 'xxx')

    # envs for parsing environment variable values example
    setenv.set('V0', '0')
    setenv.set('SUB_MODEL', '{"v1": "json-1", "v2": "json-2"}')
    setenv.set('SUB_MODEL__V2', 'nested-2')
    setenv.set('SUB_MODEL__V3', '3')
    setenv.set('SUB_MODEL__DEEP__V4', 'v4')

    # envs for parsing environment variable values example with env_nested_max_split=1
    setenv.set('GENERATION_LLM_PROVIDER', 'anthropic')
    setenv.set('GENERATION_LLM_API_KEY', 'your-api-key')
    setenv.set('GENERATION_LLM_API_VERSION', '2024-03-15')

    yield setenv

    setenv.clear()


@pytest.fixture
def cli_test_env():
    setenv = SetEnv()

    # envs for reproducible cli tests
    setenv.set('COLUMNS', '80')

    yield setenv

    setenv.clear()
