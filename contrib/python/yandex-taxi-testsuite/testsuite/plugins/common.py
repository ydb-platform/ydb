import json
import pathlib
import typing

import pytest

from testsuite import types
from testsuite.utils import cached_property, json_util, traceback, yaml_util


class BaseError(Exception):
    """Base class for errors from this module."""


class UnsupportedFileModeError(BaseError):
    """Unsupported file open mode passed."""


class LoadJsonError(BaseError):
    """Json file load or parse failure error."""


class LoadYamlError(BaseError):
    """Yaml file load or parse failure error."""


__tracebackhide__ = traceback.hide(BaseError, FileNotFoundError)


class GetSearchPathsFixture(typing.Protocol):
    """Generates sequence of paths for static files."""

    def __call__(
        self,
        filename: types.PathOrStr,
    ) -> typing.Iterator[pathlib.Path]: ...


class SearchPathFixture(typing.Protocol):
    def __call__(
        self,
        filename: types.PathOrStr,
        directory: bool = False,
    ) -> typing.Iterator[pathlib.Path]: ...


class GetFilePathFixture(typing.Protocol):
    """Returns path to static regular file."""

    def __call__(
        self,
        filename: types.PathOrStr,
        *,
        missing_ok=False,
    ) -> pathlib.Path | None: ...


class GetDirectoryPathFixture(typing.Protocol):
    """Returns path to static directory."""

    def __call__(
        self,
        filename: types.PathOrStr,
        *,
        missing_ok=False,
    ) -> pathlib.Path | None: ...


class OpenFileFixture(typing.Protocol):
    """Open static file by name.

    Only read-only open modes are supported.

    Example:

    .. code-block:: python

        def test_foo(open_file):
            with open_file('foo') as fp:
                ...
    """

    def __call__(
        self,
        filename: types.PathOrStr,
        mode='r',
        buffering=-1,
        encoding='utf-8',
        errors=None,
    ) -> typing.IO: ...


class LoadFixture(typing.Protocol):
    """Load file from static directory.

    Example:

    .. code-block:: python

        def test_something(load):
            data = load('filename')

    :return: :py:class:`LoadFixture` callable instance.
    """

    def __call__(
        self,
        filename: types.PathOrStr,
        encoding='utf-8',
        errors=None,
        *,
        missing_ok=False,
    ) -> bytes | str | None: ...


class LoadBinaryFixture(typing.Protocol):
    """Load binary data from static directory.

    Example:

    .. code-block:: python

        def test_something(load_binary):
            bytes_data = load_binary('data.bin')
    """

    def __call__(self, filename: types.PathOrStr) -> bytes: ...


class JsonLoadsFixture(typing.Protocol):
    """Load json doc from string.

    Json loader runs ``json_util.loads(data, ..., *args, **kwargs)`` hooks.
    It does:
    * bson.json_util.object_hook()
    * mockserver substitution

    Example:

    .. code-block:: python

        def test_something(json_loads):
            json_obj = json_loads('{"key": "value"}')
    """

    def __call__(self, content, *args, **kwargs) -> typing.Any: ...


class LoadJsonFixture(typing.Protocol):
    """Load json doc from static directory.

    Json loader runs ``json_util.loads(data, ..., *args, **kwargs)`` hooks.
    It does:
    * bson.json_util.object_hook()
    * mockserver substitution

    Example:

    .. code-block:: python

        def test_something(load_json):
            json_obj = load_json('filename.json')
    """

    def __call__(
        self,
        filename: types.PathOrStr,
        *args,
        missing_ok=False,
        missing=None,
        **kwargs,
    ) -> typing.Any: ...


class LoadYamlFixture(typing.Protocol):
    """Load yaml doc from static directory.

    .. code-block:: python

        def test_something(load_yaml):
            yaml_obj = load_yaml('filename.yaml')
    """

    def __call__(
        self,
        filename: types.PathOrStr,
        *args,
        **kwargs,
    ) -> typing.Any: ...


_MODES_WHITELIST = frozenset(['r', 'rt', 'rb'])


@pytest.fixture
def get_search_pathes(
    _search_directories_existing: tuple[pathlib.Path, ...],
    _path_entries_cache: typing.Callable,
) -> GetSearchPathsFixture:
    def search(filename: types.PathOrStr) -> typing.Iterator[pathlib.Path]:
        for directory in _search_directories_existing:
            entry = _path_entries_cache(directory, filename)
            if entry.exists():
                yield entry

    return search


@pytest.fixture
def get_search_paths(get_search_pathes):
    return get_search_pathes


@pytest.fixture
def search_path(get_search_paths: GetSearchPathsFixture) -> SearchPathFixture:
    def search_path(
        filename: types.PathOrStr,
        directory: bool = False,
    ) -> typing.Iterator[pathlib.Path]:
        for abs_filename in get_search_paths(filename):
            if directory:
                if abs_filename.is_dir():
                    yield abs_filename
            else:
                if abs_filename.is_file():
                    yield abs_filename

    return search_path


@pytest.fixture
def get_file_path(
    search_path: SearchPathFixture, _testsuite_file_not_found_error
) -> GetFilePathFixture:
    def get_file_path(
        filename: types.PathOrStr,
        *,
        missing_ok=False,
    ) -> pathlib.Path | None:
        for path in search_path(filename):
            return path
        if missing_ok:
            return None
        raise _testsuite_file_not_found_error(
            f'File {filename} was not found',
            filename,
        )

    return get_file_path


@pytest.fixture
def get_directory_path(
    search_path: SearchPathFixture,
    _testsuite_file_not_found_error,
) -> GetDirectoryPathFixture:
    def get_directory_path(
        filename: types.PathOrStr,
        *,
        missing_ok=False,
    ) -> pathlib.Path | None:
        for path in search_path(filename, directory=True):
            return path
        if missing_ok:
            return None
        raise _testsuite_file_not_found_error(
            f'Directory {filename} was not found',
            filename,
        )

    return get_directory_path


@pytest.fixture
def _testsuite_file_not_found_error(_search_directories_existing):
    def raise_error(message, filename):
        paths = '\n'.join(
            f' - {path / filename}' for path in _search_directories_existing
        )
        return FileNotFoundError(
            f'{message}\n\nThe following paths were examined:\n{paths}',
        )

    return raise_error


@pytest.fixture
def open_file(get_file_path: GetFilePathFixture) -> OpenFileFixture:
    def open_file(
        filename: types.PathOrStr,
        mode='r',
        buffering=-1,
        encoding='utf-8',
        errors=None,
    ) -> typing.IO:
        if mode not in _MODES_WHITELIST:
            raise UnsupportedFileModeError(
                f'Incorrect file open mode {mode!r} passed. '
                f'Only read-only modes are supported.',
            )
        return open(
            get_file_path(filename),  # type: ignore[arg-type]
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
        )

    return open_file


@pytest.fixture
def load(get_file_path: GetFilePathFixture) -> LoadFixture:
    def load(
        filename: types.PathOrStr,
        encoding='utf-8',
        errors=None,
        *,
        missing_ok=False,
    ) -> bytes | str | None:
        path = get_file_path(filename, missing_ok=missing_ok)
        if path is None:
            return None
        return path.read_text(encoding=encoding, errors=errors)

    return load


@pytest.fixture
def load_binary(get_file_path: GetFilePathFixture) -> LoadBinaryFixture:
    def load_binary(filename: types.PathOrStr) -> bytes:
        path = get_file_path(filename)
        return path.read_bytes()

    return load_binary


@pytest.fixture
def json_loads(object_hook, load_json_defaults) -> JsonLoadsFixture:
    def json_loads(content, *args, **kwargs) -> typing.Any:
        if 'object_hook' not in kwargs:
            kwargs['object_hook'] = object_hook

        return json_util.loads(
            content,
            *args,
            **load_json_defaults,
            **kwargs,
        )

    return json_loads


@pytest.fixture
def load_json(
    load: LoadFixture, json_loads: JsonLoadsFixture
) -> LoadJsonFixture:
    def load_json(
        filename: types.PathOrStr,
        *args,
        missing_ok=False,
        missing=None,
        **kwargs,
    ) -> typing.Any:
        content = load(filename, missing_ok=missing_ok)
        if content is None:
            return missing
        try:
            return json_loads(content, *args, **kwargs)
        except json.JSONDecodeError as err:
            raise LoadJsonError(
                f'Failed to load JSON file {filename}',
            ) from err

    return load_json


@pytest.fixture
def load_yaml(load: LoadFixture) -> LoadYamlFixture:
    def load_yaml(
        filename: types.PathOrStr,
        *args,
        **kwargs,
    ) -> typing.Any:
        content = load(filename)
        try:
            return yaml_util.load(content, *args, **kwargs)
        except yaml_util.ParserError as exc:
            raise LoadYamlError(
                f'Failed to load YAML file {filename}',
            ) from exc

    return load_yaml


FilePathsCache = dict[pathlib.Path, list[pathlib.Path]]


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'nofilldb: test does not need db initialization',
    )


@pytest.fixture
def static_dir(testsuite_request_directory) -> pathlib.Path:
    """Static directory related to test path.

    Returns static directory relative to test file, e.g.::

       |- tests/
          |- static/ <-- base static directory for test_foo.py
          |- test_foo.py

    """
    return testsuite_request_directory / 'static'


@pytest.fixture
def initial_data_path() -> tuple[pathlib.Path, ...]:
    """Use this fixture to override base static search path.

    .. code-block:: python

     @pytest.fixture
     def initial_data_path():
         return (
             pathlib.Path(PROJECT_ROOT) / 'tests/static',
             pathlib.Path(PROJECT_ROOT) / 'static',
         )
    """

    return ()


@pytest.fixture
def get_all_static_file_paths(
    static_dir: pathlib.Path,
    _file_paths_cache: FilePathsCache,
):
    def _get_file_paths() -> list[pathlib.Path]:
        if static_dir not in _file_paths_cache:
            _file_paths_cache[static_dir] = [
                path for path in static_dir.rglob('') if path.is_file()
            ]
        return _file_paths_cache[static_dir]

    return _get_file_paths


@pytest.fixture
def object_substitute(object_hook):
    """Perform object substitution as in load_json."""

    def _substitute(content, *args, **kwargs):
        return json_util.substitute(
            content,
            object_hook=object_hook,
            *args,
            **kwargs,
        )

    return _substitute


@pytest.fixture(scope='session')
def testsuite_get_source_path():
    def get_source_path(path) -> pathlib.Path:
        return pathlib.Path(path)

    return get_source_path


@pytest.fixture(scope='session')
def testsuite_get_source_directory(testsuite_get_source_path):
    def get_source_directory(path) -> pathlib.Path:
        return testsuite_get_source_path(path).parent

    return get_source_directory


@pytest.fixture
def testsuite_request_path(request, testsuite_get_source_path) -> pathlib.Path:
    return testsuite_get_source_path(request.module.__file__)


@pytest.fixture
def testsuite_request_directory(testsuite_request_path) -> pathlib.Path:
    return testsuite_request_path.parent


@pytest.fixture(scope='session')
def worker_id(request) -> str:
    if hasattr(request.config, 'workerinput'):
        return request.config.workerinput['workerid']
    return 'master'


@pytest.fixture(scope='session')
def _file_paths_cache() -> FilePathsCache:
    return {}


@pytest.fixture
def _search_directories(
    request,
    static_dir: pathlib.Path,
    initial_data_path: tuple[pathlib.Path, ...],
    testsuite_request_path,
    _path_entries_cache,
) -> tuple[pathlib.Path, ...]:
    test_module_name = testsuite_request_path.stem
    node_name = request.node.name
    if '[' in node_name:
        node_name = node_name[: node_name.index('[')]
    search_directories = [
        _path_entries_cache(static_dir, test_module_name, node_name),
        _path_entries_cache(static_dir, test_module_name),
        _path_entries_cache(static_dir, 'default'),
        _path_entries_cache(static_dir, ''),
    ]
    search_directories.extend(
        _path_entries_cache(path) for path in initial_data_path
    )
    return tuple(search_directories)


@pytest.fixture
def _search_directories_existing(_search_directories):
    return tuple(path for path in _search_directories if path.is_dir())


@pytest.fixture(scope='session')
def load_json_defaults():
    return {}


@pytest.fixture(scope='session')
def _cached_stat_path():
    path_type = type(pathlib.Path())
    stat_cache: dict[str, tuple[bool, FileNotFoundError | typing.Any]] = {}
    glob_cache: dict[typing.Any, tuple] = {}
    content_cache: dict[typing.Any, tuple | str | bytes] = {}
    iterdir_cache: dict[str, tuple] = {}

    class CachedStatPath(path_type):  # type: ignore[valid-type]
        def stat(self, *, follow_symlinks=True):
            key = str(self)
            if key in stat_cache:
                is_exc, value = stat_cache[key]
                if is_exc:
                    raise value
                return value
            try:
                value = super().stat()
                stat_cache[key] = (False, value)
                return value
            except FileNotFoundError as exc:
                stat_cache[key] = (True, exc)
                raise

        def iterdir(self):
            cache_key = str(self)
            data = iterdir_cache.get(cache_key)
            if data is None:
                data = tuple(super().iterdir())
                content_cache[cache_key] = data
            return data

        def read_bytes(self):
            cache_key = (str(self), 'b')
            data = content_cache.get(cache_key)
            if data is None:
                data = super().read_bytes()
                content_cache[cache_key] = data
            return data

        def read_text(self, encoding=None, errors=None):
            cache_key = (str(self), encoding, errors)
            data = content_cache.get(cache_key)
            if data is None:
                data = super().read_text(encoding=encoding, errors=errors)
                content_cache[cache_key] = data
            return data

        def glob(self, pattern):
            key = (str(self), pattern)
            if key not in glob_cache:
                data = glob_cache[key] = tuple(super().glob(pattern))
                return data
            return glob_cache[key]

        def rglob(self, pattern):
            key = ('r', str(self), pattern)
            if key not in glob_cache:
                data = glob_cache[key] = tuple(super().rglob(pattern))
                return data
            return glob_cache[key]

        def exists(self):
            return self._exists

        def is_dir(self):
            return self._is_dir

        def is_file(self):
            return self._is_file

        @cached_property
        def _is_dir(self):
            return super().is_dir()

        @cached_property
        def _is_file(self):
            return super().is_file()

        @cached_property
        def _exists(self):
            return super().exists()

    return CachedStatPath


@pytest.fixture(scope='session')
def _path_entries_cache(_cached_stat_path):
    entries_cache = {}

    def get(*parts):
        result = entries_cache.get(parts)
        if result:
            return result
        result = _cached_stat_path(*parts)
        entries_cache[parts] = result
        return result

    return get
