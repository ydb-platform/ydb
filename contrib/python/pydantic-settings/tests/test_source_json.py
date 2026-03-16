"""
Test pydantic_settings.JsonConfigSettingsSource.
"""

import importlib.resources
import json
import sys

if sys.version_info < (3, 11):
    from importlib.abc import Traversable
else:
    from importlib.resources.abc import Traversable

from pathlib import Path

import pytest
from pydantic import BaseModel

from pydantic_settings import (
    BaseSettings,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


def test_repr() -> None:
    source = JsonConfigSettingsSource(BaseSettings, Path('config.json'))
    assert repr(source) == 'JsonConfigSettingsSource(json_file=config.json)'


def test_json_file(tmp_path):
    p = tmp_path / '.env'
    p.write_text(
        """
    {"foobar": "Hello", "nested": {"nested_field": "world!"}, "null_field": null}
    """
    )

    class Nested(BaseModel):
        nested_field: str

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(json_file=p)
        foobar: str
        nested: Nested
        null_field: str | None

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (JsonConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.foobar == 'Hello'
    assert s.nested.nested_field == 'world!'


def test_json_no_file():
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(json_file=None)

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (JsonConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.model_dump() == {}


def test_multiple_file_json(tmp_path):
    p5 = tmp_path / '.env.json5'
    p6 = tmp_path / '.env.json6'

    with open(p5, 'w') as f5:
        json.dump({'json5': 5}, f5)
    with open(p6, 'w') as f6:
        json.dump({'json6': 6}, f6)

    class Settings(BaseSettings):
        json5: int
        json6: int

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (JsonConfigSettingsSource(settings_cls, json_file=[p5, p6]),)

    s = Settings()
    assert s.model_dump() == {'json5': 5, 'json6': 6}


@pytest.mark.parametrize('deep_merge', [False, True])
def test_multiple_file_json_merge(tmp_path, deep_merge):
    p5 = tmp_path / '.env.json5'
    p6 = tmp_path / '.env.json6'

    with open(p5, 'w') as f5:
        json.dump({'hello': 'world', 'nested': {'foo': 1, 'bar': 2}}, f5)
    with open(p6, 'w') as f6:
        json.dump({'nested': {'foo': 3}}, f6)

    class Nested(BaseModel):
        foo: int
        bar: int = 0

    class Settings(BaseSettings):
        hello: str
        nested: Nested

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (JsonConfigSettingsSource(settings_cls, json_file=[p5, p6], deep_merge=deep_merge),)

    s = Settings()
    assert s.model_dump() == {'hello': 'world', 'nested': {'foo': 3, 'bar': 2 if deep_merge else 0}}


class TestTraversableSupport:
    FILENAME = 'example_test_config.json'

    @pytest.fixture(params=['custom_with_path'])
    def json_config_path(self, request, tmp_path):

        # Create a custom Traversable implementation
        class CustomTraversable(Traversable):
            def __init__(self, path):
                self._path = path

            def __truediv__(self, child):
                return CustomTraversable(self._path / child)

            def is_file(self):
                return self._path.is_file()

            def is_dir(self):
                return self._path.is_dir()

            def iterdir(self):
                raise NotImplementedError('iterdir not implemented for this test')

            def open(self, mode='r', *args, **kwargs):
                return self._path.open(mode, *args, **kwargs)

            def read_bytes(self):
                return self._path.read_bytes()

            def read_text(self, encoding=None):
                return self._path.read_text(encoding=encoding)

            @property
            def name(self):
                return self._path.name

            def joinpath(self, *descendants):
                return CustomTraversable(self._path.joinpath(*descendants))


        filepath = tmp_path / self.FILENAME
        with filepath.open('w') as f:
            json.dump({'foobar': 'test'}, f)
        return CustomTraversable(filepath)

    def test_traversable_support(self, json_config_path: Traversable):
        assert json_config_path.is_file()

        class Settings(BaseSettings):
            foobar: str

            model_config = SettingsConfigDict(
                # Traversable is not added in annotation, but is supported
                json_file=json_config_path,
            )

            @classmethod
            def settings_customise_sources(
                cls,
                settings_cls: type[BaseSettings],
                init_settings: PydanticBaseSettingsSource,
                env_settings: PydanticBaseSettingsSource,
                dotenv_settings: PydanticBaseSettingsSource,
                file_secret_settings: PydanticBaseSettingsSource,
            ) -> tuple[PydanticBaseSettingsSource, ...]:
                return (JsonConfigSettingsSource(settings_cls),)

        s = Settings()
        # "test" value in file
        assert s.foobar == 'test'
