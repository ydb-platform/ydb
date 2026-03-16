"""
Test pydantic_settings.TomlConfigSettingsSource.
"""

import sys
from pathlib import Path

import pytest
from pydantic import BaseModel

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

try:
    import tomli
except ImportError:
    tomli = None


def test_repr() -> None:
    source = TomlConfigSettingsSource(BaseSettings, Path('config.toml'))
    assert repr(source) == 'TomlConfigSettingsSource(toml_file=config.toml)'


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
def test_toml_file(tmp_path):
    p = tmp_path / '.env'
    p.write_text(
        """
    foobar = "Hello"

    [nested]
    nested_field = "world!"
    """
    )

    class Nested(BaseModel):
        nested_field: str

    class Settings(BaseSettings):
        foobar: str
        nested: Nested
        model_config = SettingsConfigDict(toml_file=p)

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (TomlConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.foobar == 'Hello'
    assert s.nested.nested_field == 'world!'


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
def test_toml_no_file():
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(toml_file=None)

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (TomlConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.model_dump() == {}


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
def test_multiple_file_toml(tmp_path):
    p1 = tmp_path / '.env.toml1'
    p2 = tmp_path / '.env.toml2'
    p1.write_text(
        """
    toml1=1
    """
    )
    p2.write_text(
        """
    toml2=2
    """
    )

    class Settings(BaseSettings):
        toml1: int
        toml2: int

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (TomlConfigSettingsSource(settings_cls, toml_file=[p1, p2]),)

    s = Settings()
    assert s.model_dump() == {'toml1': 1, 'toml2': 2}


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
@pytest.mark.parametrize('deep_merge', [False, True])
def test_multiple_file_toml_merge(tmp_path, deep_merge):
    p1 = tmp_path / '.env.toml1'
    p2 = tmp_path / '.env.toml2'
    p1.write_text(
        """
    hello = "world"

    [nested]
    foo=1
    bar=2
    """
    )
    p2.write_text(
        """
    [nested]
    foo=3
    """
    )

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
            return (TomlConfigSettingsSource(settings_cls, toml_file=[p1, p2], deep_merge=deep_merge),)

    s = Settings()
    assert s.model_dump() == {'hello': 'world', 'nested': {'foo': 3, 'bar': 2 if deep_merge else 0}}
