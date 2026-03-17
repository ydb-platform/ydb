"""
Test pydantic_settings.PyprojectTomlConfigSettingsSource.
"""

import sys
from pathlib import Path

import pytest
from pydantic import BaseModel
from pytest_mock import MockerFixture

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
)

try:
    import tomli
except ImportError:
    tomli = None


MODULE = 'pydantic_settings.sources.providers.pyproject'

SOME_TOML_DATA = """
field = "top-level"

[some]
[some.table]
field = "some"

[other.table]
field = "other"
"""


class SimpleSettings(BaseSettings):
    """Simple settings."""

    model_config = SettingsConfigDict(pyproject_toml_depth=1, pyproject_toml_table_header=('some', 'table'))


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
class TestPyprojectTomlConfigSettingsSource:
    """Test PyprojectTomlConfigSettingsSource."""

    def test___init__(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test __init__."""
        mocker.patch(f'{MODULE}.Path.cwd', return_value=tmp_path)
        pyproject = tmp_path / 'pyproject.toml'
        pyproject.write_text(SOME_TOML_DATA)
        obj = PyprojectTomlConfigSettingsSource(SimpleSettings)
        assert obj.toml_table_header == ('some', 'table')
        assert obj.toml_data == {'field': 'some'}
        assert obj.toml_file_path == tmp_path / 'pyproject.toml'

    def test___init___explicit(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test __init__ explicit file."""
        mocker.patch(f'{MODULE}.Path.cwd', return_value=tmp_path)
        pyproject = tmp_path / 'child' / 'pyproject.toml'
        pyproject.parent.mkdir()
        pyproject.write_text(SOME_TOML_DATA)
        obj = PyprojectTomlConfigSettingsSource(SimpleSettings, pyproject)
        assert obj.toml_table_header == ('some', 'table')
        assert obj.toml_data == {'field': 'some'}
        assert obj.toml_file_path == pyproject

    def test___init___explicit_missing(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test __init__ explicit file missing."""
        mocker.patch(f'{MODULE}.Path.cwd', return_value=tmp_path)
        pyproject = tmp_path / 'child' / 'pyproject.toml'
        obj = PyprojectTomlConfigSettingsSource(SimpleSettings, pyproject)
        assert obj.toml_table_header == ('some', 'table')
        assert not obj.toml_data
        assert obj.toml_file_path == pyproject

    @pytest.mark.parametrize('depth', [0, 99])
    def test___init___no_file(self, depth: int, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test __init__ no file."""

        class Settings(BaseSettings):
            model_config = SettingsConfigDict(pyproject_toml_depth=depth)

        mocker.patch(f'{MODULE}.Path.cwd', return_value=tmp_path / 'foo')
        obj = PyprojectTomlConfigSettingsSource(Settings)
        assert obj.toml_table_header == ('tool', 'pydantic-settings')
        assert not obj.toml_data
        assert obj.toml_file_path == tmp_path / 'foo' / 'pyproject.toml'

    def test___init___parent(self, mocker: MockerFixture, tmp_path: Path) -> None:
        """Test __init__ parent directory."""
        mocker.patch(f'{MODULE}.Path.cwd', return_value=tmp_path / 'child')
        pyproject = tmp_path / 'pyproject.toml'
        pyproject.write_text(SOME_TOML_DATA)
        obj = PyprojectTomlConfigSettingsSource(SimpleSettings)
        assert obj.toml_table_header == ('some', 'table')
        assert obj.toml_data == {'field': 'some'}
        assert obj.toml_file_path == tmp_path / 'pyproject.toml'


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
def test_pyproject_toml_file(cd_tmp_path: Path):
    pyproject = cd_tmp_path / 'pyproject.toml'
    pyproject.write_text(
        """
    [tool.pydantic-settings]
    foobar = "Hello"

    [tool.pydantic-settings.nested]
    nested_field = "world!"
    """
    )

    class Nested(BaseModel):
        nested_field: str

    class Settings(BaseSettings):
        foobar: str
        nested: Nested
        model_config = SettingsConfigDict()

        @classmethod
        def settings_customise_sources(
            cls, settings_cls: type[BaseSettings], **_kwargs: PydanticBaseSettingsSource
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (PyprojectTomlConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.foobar == 'Hello'
    assert s.nested.nested_field == 'world!'


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
def test_pyproject_toml_file_explicit(cd_tmp_path: Path):
    pyproject = cd_tmp_path / 'child' / 'grandchild' / 'pyproject.toml'
    pyproject.parent.mkdir(parents=True)
    pyproject.write_text(
        """
    [tool.pydantic-settings]
    foobar = "Hello"

    [tool.pydantic-settings.nested]
    nested_field = "world!"
    """
    )
    (cd_tmp_path / 'pyproject.toml').write_text(
        """
    [tool.pydantic-settings]
    foobar = "fail"

    [tool.pydantic-settings.nested]
    nested_field = "fail"
    """
    )

    class Nested(BaseModel):
        nested_field: str

    class Settings(BaseSettings):
        foobar: str
        nested: Nested
        model_config = SettingsConfigDict()

        @classmethod
        def settings_customise_sources(
            cls, settings_cls: type[BaseSettings], **_kwargs: PydanticBaseSettingsSource
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (PyprojectTomlConfigSettingsSource(settings_cls, pyproject),)

    s = Settings()
    assert s.foobar == 'Hello'
    assert s.nested.nested_field == 'world!'


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
def test_pyproject_toml_file_parent(mocker: MockerFixture, tmp_path: Path):
    cwd = tmp_path / 'child' / 'grandchild' / 'cwd'
    cwd.mkdir(parents=True)
    mocker.patch('pydantic_settings.sources.providers.toml.Path.cwd', return_value=cwd)
    (cwd.parent.parent / 'pyproject.toml').write_text(
        """
    [tool.pydantic-settings]
    foobar = "Hello"

    [tool.pydantic-settings.nested]
    nested_field = "world!"
    """
    )
    (tmp_path / 'pyproject.toml').write_text(
        """
    [tool.pydantic-settings]
    foobar = "fail"

    [tool.pydantic-settings.nested]
    nested_field = "fail"
    """
    )

    class Nested(BaseModel):
        nested_field: str

    class Settings(BaseSettings):
        foobar: str
        nested: Nested
        model_config = SettingsConfigDict(pyproject_toml_depth=2)

        @classmethod
        def settings_customise_sources(
            cls, settings_cls: type[BaseSettings], **_kwargs: PydanticBaseSettingsSource
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (PyprojectTomlConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.foobar == 'Hello'
    assert s.nested.nested_field == 'world!'


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
def test_pyproject_toml_file_header(cd_tmp_path: Path):
    pyproject = cd_tmp_path / 'subdir' / 'pyproject.toml'
    pyproject.parent.mkdir()
    pyproject.write_text(
        """
    [tool.pydantic-settings]
    foobar = "Hello"

    [tool.pydantic-settings.nested]
    nested_field = "world!"

    [tool."my.tool".foo]
    status = "success"
    """
    )

    class Settings(BaseSettings):
        status: str
        model_config = SettingsConfigDict(extra='forbid', pyproject_toml_table_header=('tool', 'my.tool', 'foo'))

        @classmethod
        def settings_customise_sources(
            cls, settings_cls: type[BaseSettings], **_kwargs: PydanticBaseSettingsSource
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (PyprojectTomlConfigSettingsSource(settings_cls, pyproject),)

    s = Settings()
    assert s.status == 'success'


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
@pytest.mark.parametrize('depth', [0, 99])
def test_pyproject_toml_no_file(cd_tmp_path: Path, depth: int):
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(pyproject_toml_depth=depth)

        @classmethod
        def settings_customise_sources(
            cls, settings_cls: type[BaseSettings], **_kwargs: PydanticBaseSettingsSource
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (PyprojectTomlConfigSettingsSource(settings_cls),)

    s = Settings()
    assert s.model_dump() == {}


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
def test_pyproject_toml_no_file_explicit(tmp_path: Path):
    pyproject = tmp_path / 'child' / 'pyproject.toml'
    (tmp_path / 'pyproject.toml').write_text('[tool.pydantic-settings]\nfield = "fail"')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict()

        field: str | None = None

        @classmethod
        def settings_customise_sources(
            cls, settings_cls: type[BaseSettings], **_kwargs: PydanticBaseSettingsSource
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (PyprojectTomlConfigSettingsSource(settings_cls, pyproject),)

    s = Settings()
    assert s.model_dump() == {'field': None}


@pytest.mark.skipif(sys.version_info <= (3, 11) and tomli is None, reason='tomli/tomllib is not installed')
@pytest.mark.parametrize('depth', [0, 1, 2])
def test_pyproject_toml_no_file_too_shallow(depth: int, mocker: MockerFixture, tmp_path: Path):
    cwd = tmp_path / 'child' / 'grandchild' / 'cwd'
    cwd.mkdir(parents=True)
    mocker.patch('pydantic_settings.sources.providers.toml.Path.cwd', return_value=cwd)
    (tmp_path / 'pyproject.toml').write_text(
        """
    [tool.pydantic-settings]
    foobar = "fail"

    [tool.pydantic-settings.nested]
    nested_field = "fail"
    """
    )

    class Nested(BaseModel):
        nested_field: str | None = None

    class Settings(BaseSettings):
        foobar: str | None = None
        nested: Nested = Nested()
        model_config = SettingsConfigDict(pyproject_toml_depth=depth)

        @classmethod
        def settings_customise_sources(
            cls, settings_cls: type[BaseSettings], **_kwargs: PydanticBaseSettingsSource
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (PyprojectTomlConfigSettingsSource(settings_cls),)

    s = Settings()
    assert not s.foobar
    assert not s.nested.nested_field
