import collections.abc
import pathlib
import typing

from testsuite import types
from testsuite.utils import yaml_util


class MongoSchema(collections.abc.Mapping):
    _directory: pathlib.Path
    _loaded: dict[str, dict]
    _paths: dict[str, pathlib.Path]

    def __init__(self, directory: types.PathOrStr) -> None:
        self._directory = pathlib.Path(directory)
        self._loaded = {}
        self._paths = _get_paths(self._directory)

    def __getitem__(self, name: str) -> dict:
        if name not in self._paths:
            raise KeyError(f'Missing schema file for collection {name}')
        if name not in self._loaded:
            self._loaded[name] = yaml_util.load_file(self._paths[name])
        return self._loaded[name]

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._paths)

    def __len__(self) -> int:
        return len(self._paths)

    @property
    def directory(self) -> pathlib.Path:
        return self._directory


class MongoSchemaCache:
    def __init__(self) -> None:
        self._cache: dict[pathlib.Path, MongoSchema] = {}

    def get_schema(self, directory: types.PathOrStr) -> MongoSchema:
        directory = pathlib.Path(directory)
        if directory not in self._cache:
            self._cache[directory] = MongoSchema(directory)
        return self._cache[directory]


class MongoSchemas(collections.abc.Mapping):
    def __init__(
        self,
        cache: MongoSchemaCache,
        directories: typing.Iterable[types.PathOrStr],
    ):
        self._cache = cache
        self._directories = [
            pathlib.Path(directory) for directory in directories
        ]
        self._schema_by_collection: dict[str, MongoSchema] = {}
        for directory in self._directories:
            schema = cache.get_schema(directory)
            for name in schema:
                if name in self._schema_by_collection:
                    raise RuntimeError(
                        f'Duplicate definition of collection {name}:\n'
                        f'  at {self._schema_by_collection[name].directory}\n'
                        f'  at {directory}',
                    )
                self._schema_by_collection[name] = schema

    def __getitem__(self, name):
        if name not in self._schema_by_collection:
            raise KeyError(f'Missing schema file for collection {name}')
        return self._schema_by_collection[name][name]

    def __iter__(self):
        for directory in self._directories:
            yield from self._cache.get_schema(directory)

    def __len__(self) -> int:
        return sum(
            len(self._cache.get_schema(directory))
            for directory in self._directories
        )


def _get_paths(directory: pathlib.Path) -> dict[str, pathlib.Path]:
    return {path.stem: path for path in directory.glob('*.yaml')}
