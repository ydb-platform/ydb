import dataclasses
import json
import os
import pathlib
import sys
import uuid
from collections.abc import Callable, Hashable
from datetime import date, datetime, timezone
from enum import IntEnum
from pathlib import Path
from typing import Annotated, Any, Generic, Literal, TypeVar
from unittest import mock

import pytest
from annotated_types import MinLen
from pydantic import (
    AliasChoices,
    AliasGenerator,
    AliasPath,
    BaseModel,
    Discriminator,
    Field,
    HttpUrl,
    Json,
    PostgresDsn,
    RootModel,
    Secret,
    SecretStr,
    Tag,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic import (
    dataclasses as pydantic_dataclasses,
)
from pydantic.fields import FieldInfo
from typing_extensions import TypeAliasType, override

from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    EnvSettingsSource,
    ForceDecode,
    InitSettingsSource,
    NoDecode,
    PydanticBaseSettingsSource,
    SecretsSettingsSource,
    SettingsConfigDict,
    SettingsError,
)
from pydantic_settings.sources import DefaultSettingsSource

try:
    import dotenv
except ImportError:
    dotenv = None


class FruitsEnum(IntEnum):
    pear = 0
    kiwi = 1
    lime = 2


class SimpleSettings(BaseSettings):
    apple: str


class SettingWithIgnoreEmpty(BaseSettings):
    apple: str = 'default'

    model_config = SettingsConfigDict(env_ignore_empty=True)


class SettingWithPopulateByName(BaseSettings):
    apple: str = Field('default', alias='pomo')

    model_config = SettingsConfigDict(populate_by_name=True)


@pytest.fixture(autouse=True)
def clean_env():
    with mock.patch.dict(os.environ, clear=True):
        yield


def test_sub_env(env):
    env.set('apple', 'hello')
    s = SimpleSettings()
    assert s.apple == 'hello'


def test_sub_env_override(env):
    env.set('apple', 'hello')
    s = SimpleSettings(apple='goodbye')
    assert s.apple == 'goodbye'


def test_sub_env_missing():
    with pytest.raises(ValidationError) as exc_info:
        SimpleSettings()
    assert exc_info.value.errors(include_url=False) == [
        {'type': 'missing', 'loc': ('apple',), 'msg': 'Field required', 'input': {}}
    ]


def test_other_setting():
    with pytest.raises(ValidationError):
        SimpleSettings(apple='a', foobar=42)


def test_ignore_empty_when_empty_uses_default(env):
    env.set('apple', '')
    s = SettingWithIgnoreEmpty()
    assert s.apple == 'default'


def test_ignore_empty_when_not_empty_uses_value(env):
    env.set('apple', 'a')
    s = SettingWithIgnoreEmpty()
    assert s.apple == 'a'


def test_ignore_empty_with_dotenv_when_empty_uses_default(tmp_path):
    p = tmp_path / '.env'
    p.write_text('a=')

    class Settings(BaseSettings):
        a: str = 'default'

        model_config = SettingsConfigDict(env_file=p, env_ignore_empty=True)

    s = Settings()
    assert s.a == 'default'


def test_ignore_empty_with_dotenv_when_not_empty_uses_value(tmp_path):
    p = tmp_path / '.env'
    p.write_text('a=b')

    class Settings(BaseSettings):
        a: str = 'default'

        model_config = SettingsConfigDict(env_file=p, env_ignore_empty=True)

    s = Settings()
    assert s.a == 'b'


def test_populate_by_name_when_using_alias(env):
    env.set('pomo', 'bongusta')
    s = SettingWithPopulateByName()
    assert s.apple == 'bongusta'


def test_populate_by_name_when_using_name(env):
    env.set('apple', 'honeycrisp')
    s = SettingWithPopulateByName()
    assert s.apple == 'honeycrisp'


def test_populate_by_name_when_using_both(env):
    env.set('apple', 'honeycrisp')
    env.set('pomo', 'bongusta')
    s = SettingWithPopulateByName()
    assert s.apple == 'bongusta', 'Expected alias value to be prioritized.'


def test_populate_by_name_with_alias_path_when_using_alias(env):
    env.set('fruits', '["empire", "honeycrisp"]')

    class Settings(BaseSettings):
        apple: str = Field('default', validation_alias=AliasPath('fruits', 0))
        model_config = SettingsConfigDict(populate_by_name=True)

    s = Settings()
    assert s.apple == 'empire'


def test_populate_by_name_with_alias_path_when_using_name(env):
    env.set('apple', 'jonathan gold')

    class Settings(BaseSettings):
        apple: str = Field('default', validation_alias=AliasPath('fruits', 0))
        model_config = SettingsConfigDict(populate_by_name=True)

    s = Settings()
    assert s.apple == 'jonathan gold'


@pytest.mark.parametrize(
    'env_vars, expected_value',
    [
        pytest.param({'pomo': 'pomo-chosen'}, 'pomo-chosen', id='pomo'),
        pytest.param({'pomme': 'pomme-chosen'}, 'pomme-chosen', id='pomme'),
        pytest.param({'manzano': 'manzano-chosen'}, 'manzano-chosen', id='manzano'),
        pytest.param(
            {'pomo': 'pomo-chosen', 'pomme': 'pomme-chosen', 'manzano': 'manzano-chosen'},
            'pomo-chosen',
            id='pomo-priority',
        ),
        pytest.param({'pomme': 'pomme-chosen', 'manzano': 'manzano-chosen'}, 'pomme-chosen', id='pomme-priority'),
    ],
)
def test_populate_by_name_with_alias_choices_when_using_alias(env, env_vars: dict[str, str], expected_value: str):
    for k, v in env_vars.items():
        env.set(k, v)

    class Settings(BaseSettings):
        apple: str = Field('default', validation_alias=AliasChoices('pomo', 'pomme', 'manzano'))
        model_config = SettingsConfigDict(populate_by_name=True)

    s = Settings()
    assert s.apple == expected_value


def test_populate_by_name_with_dotenv_when_using_alias(tmp_path):
    p = tmp_path / '.env'
    p.write_text('pomo=bongusta')

    class Settings(BaseSettings):
        apple: str = Field('default', alias='pomo')
        model_config = SettingsConfigDict(env_file=p, populate_by_name=True)

    s = Settings()
    assert s.apple == 'bongusta'


def test_populate_by_name_with_dotenv_when_using_name(tmp_path):
    p = tmp_path / '.env'
    p.write_text('apple=honeycrisp')

    class Settings(BaseSettings):
        apple: str = Field('default', alias='pomo')
        model_config = SettingsConfigDict(env_file=p, populate_by_name=True)

    s = Settings()
    assert s.apple == 'honeycrisp'


def test_populate_by_name_with_dotenv_when_using_both(tmp_path):
    p = tmp_path / '.env'
    p.write_text('apple=honeycrisp')
    p.write_text('pomo=bongusta')

    class Settings(BaseSettings):
        apple: str = Field('default', alias='pomo')
        model_config = SettingsConfigDict(env_file=p, populate_by_name=True)

    s = Settings()
    assert s.apple == 'bongusta', 'Expected alias value to be prioritized.'


def test_with_prefix(env):
    class Settings(BaseSettings):
        apple: str

        model_config = SettingsConfigDict(env_prefix='foobar_')

    with pytest.raises(ValidationError):
        Settings()
    env.set('foobar_apple', 'has_prefix')
    s = Settings()
    assert s.apple == 'has_prefix'


def test_nested_env_with_basemodel(env):
    class TopValue(BaseModel):
        apple: str
        banana: str

    class Settings(BaseSettings):
        top: TopValue

    with pytest.raises(ValidationError):
        Settings()
    env.set('top', '{"banana": "secret_value"}')
    s = Settings(top={'apple': 'value'})
    assert s.top.apple == 'value'
    assert s.top.banana == 'secret_value'


def test_merge_dict(env):
    class Settings(BaseSettings):
        top: dict[str, str]

    with pytest.raises(ValidationError):
        Settings()
    env.set('top', '{"banana": "secret_value"}')
    s = Settings(top={'apple': 'value'})
    assert s.top == {'apple': 'value', 'banana': 'secret_value'}


def test_nested_env_delimiter(env):
    class SubSubValue(BaseSettings):
        v6: str

    class SubValue(BaseSettings):
        v4: str
        v5: int
        sub_sub: SubSubValue

    class TopValue(BaseSettings):
        v1: str
        v2: str
        v3: str
        sub: SubValue

    class Cfg(BaseSettings):
        v0: str
        v0_union: SubValue | int
        top: TopValue

        model_config = SettingsConfigDict(env_nested_delimiter='__')

    env.set('top', '{"v1": "json-1", "v2": "json-2", "sub": {"v5": "xx"}}')
    env.set('top__sub__v5', '5')
    env.set('v0', '0')
    env.set('top__v2', '2')
    env.set('top__v3', '3')
    env.set('v0_union', '0')
    env.set('top__sub__sub_sub__v6', '6')
    env.set('top__sub__v4', '4')
    cfg = Cfg()
    assert cfg.model_dump() == {
        'v0': '0',
        'v0_union': 0,
        'top': {
            'v1': 'json-1',
            'v2': '2',
            'v3': '3',
            'sub': {'v4': '4', 'v5': 5, 'sub_sub': {'v6': '6'}},
        },
    }


def test_nested_env_optional_json(env):
    class Child(BaseModel):
        num_list: list[int] | None = None

    class Cfg(BaseSettings, env_nested_delimiter='__'):
        child: Child | None = None

    env.set('CHILD__NUM_LIST', '[1,2,3]')
    cfg = Cfg()
    assert cfg.model_dump() == {
        'child': {
            'num_list': [1, 2, 3],
        },
    }


def test_nested_env_delimiter_with_prefix(env):
    class Subsettings(BaseSettings):
        banana: str

    class Settings(BaseSettings):
        subsettings: Subsettings

        model_config = SettingsConfigDict(env_nested_delimiter='_', env_prefix='myprefix_')

    env.set('myprefix_subsettings_banana', 'banana')
    s = Settings()
    assert s.subsettings.banana == 'banana'

    class Settings(BaseSettings):
        subsettings: Subsettings

        model_config = SettingsConfigDict(env_nested_delimiter='_', env_prefix='myprefix__')

    env.set('myprefix__subsettings_banana', 'banana')
    s = Settings()
    assert s.subsettings.banana == 'banana'


def test_nested_env_delimiter_complex_required(env):
    class Cfg(BaseSettings):
        v: str = 'default'

        model_config = SettingsConfigDict(env_nested_delimiter='__')

    env.set('v__x', 'x')
    env.set('v__y', 'y')
    cfg = Cfg()
    assert cfg.model_dump() == {'v': 'default'}


def test_nested_env_delimiter_aliases(env):
    class SubModel(BaseModel):
        v1: str
        v2: str

    class Cfg(BaseSettings):
        sub_model: SubModel = Field(validation_alias=AliasChoices('foo', 'bar'))

        model_config = SettingsConfigDict(env_nested_delimiter='__')

    env.set('foo__v1', '-1-')
    env.set('bar__v2', '-2-')
    assert Cfg().model_dump() == {'sub_model': {'v1': '-1-', 'v2': '-2-'}}


@pytest.mark.parametrize('env_prefix', [None, 'prefix_', 'prefix__'])
def test_nested_env_max_split(env, env_prefix):
    class Person(BaseModel):
        sex: Literal['M', 'F']
        first_name: str
        date_of_birth: date

    class Cfg(BaseSettings):
        caregiver: Person
        significant_other: Person | None = None
        next_of_kin: Person | None = None

        model_config = SettingsConfigDict(env_nested_delimiter='_', env_nested_max_split=1)
        if env_prefix is not None:
            model_config['env_prefix'] = env_prefix

    env_prefix = env_prefix or ''
    env.set(env_prefix + 'caregiver_sex', 'M')
    env.set(env_prefix + 'caregiver_first_name', 'Joe')
    env.set(env_prefix + 'caregiver_date_of_birth', '1975-09-12')
    env.set(env_prefix + 'significant_other_sex', 'F')
    env.set(env_prefix + 'significant_other_first_name', 'Jill')
    env.set(env_prefix + 'significant_other_date_of_birth', '1998-04-19')
    env.set(env_prefix + 'next_of_kin_sex', 'M')
    env.set(env_prefix + 'next_of_kin_first_name', 'Jack')
    env.set(env_prefix + 'next_of_kin_date_of_birth', '1999-04-19')

    assert Cfg().model_dump() == {
        'caregiver': {'sex': 'M', 'first_name': 'Joe', 'date_of_birth': date(1975, 9, 12)},
        'significant_other': {'sex': 'F', 'first_name': 'Jill', 'date_of_birth': date(1998, 4, 19)},
        'next_of_kin': {'sex': 'M', 'first_name': 'Jack', 'date_of_birth': date(1999, 4, 19)},
    }


class DateModel(BaseModel):
    pips: bool = False


class ComplexSettings(BaseSettings):
    apples: list[str] = []
    bananas: set[int] = set()
    carrots: dict = {}
    date: DateModel = DateModel()


def test_list(env):
    env.set('apples', '["russet", "granny smith"]')
    s = ComplexSettings()
    assert s.apples == ['russet', 'granny smith']
    assert s.date.pips is False


def test_annotated_list(env):
    class AnnotatedComplexSettings(BaseSettings):
        apples: Annotated[list[str], MinLen(2)] = []

    env.set('apples', '["russet", "granny smith"]')
    s = AnnotatedComplexSettings()
    assert s.apples == ['russet', 'granny smith']

    env.set('apples', '["russet"]')
    with pytest.raises(ValidationError) as exc_info:
        AnnotatedComplexSettings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'ctx': {'actual_length': 1, 'field_type': 'List', 'min_length': 2},
            'input': ['russet'],
            'loc': ('apples',),
            'msg': 'List should have at least 2 items after validation, not 1',
            'type': 'too_short',
        }
    ]


def test_annotated_with_type(env):
    """https://github.com/pydantic/pydantic-settings/issues/536.

    PEP 695 type aliases need to be analyzed when determining if an annotation is complex.
    """
    MinLenList = TypeAliasType('MinLenList', Annotated[list[str] | list[int], MinLen(2)])

    class AnnotatedComplexSettings(BaseSettings):
        apples: MinLenList

    env.set('apples', '["russet", "granny smith"]')
    s = AnnotatedComplexSettings()
    assert s.apples == ['russet', 'granny smith']

    T = TypeVar('T')
    MinLenList = TypeAliasType('MinLenList', Annotated[list[T] | tuple[T], MinLen(2)], type_params=(T,))

    class AnnotatedComplexSettings(BaseSettings):
        apples: MinLenList[str]

    s = AnnotatedComplexSettings()
    assert s.apples == ['russet', 'granny smith']


def test_set_dict_model(env):
    env.set('bananas', '[1, 2, 3, 3]')
    env.set('CARROTS', '{"a": null, "b": 4}')
    env.set('daTE', '{"pips": true}')
    s = ComplexSettings()
    assert s.bananas == {1, 2, 3}
    assert s.carrots == {'a': None, 'b': 4}
    assert s.date.pips is True


def test_invalid_json(env):
    env.set('apples', '["russet", "granny smith",]')
    with pytest.raises(SettingsError, match='error parsing value for field "apples" from source "EnvSettingsSource"'):
        ComplexSettings()


def test_required_sub_model(env):
    class Settings(BaseSettings):
        foobar: DateModel

    with pytest.raises(ValidationError):
        Settings()
    env.set('FOOBAR', '{"pips": "TRUE"}')
    s = Settings()
    assert s.foobar.pips is True


def test_non_class(env):
    class Settings(BaseSettings):
        foobar: str | None

    env.set('FOOBAR', 'xxx')
    s = Settings()
    assert s.foobar == 'xxx'


@pytest.mark.parametrize('dataclass_decorator', (pydantic_dataclasses.dataclass, dataclasses.dataclass))
def test_generic_dataclass(env, dataclass_decorator):
    T = TypeVar('T')

    @dataclass_decorator
    class GenericDataclass(Generic[T]):
        x: T

    class ComplexSettings(BaseSettings):
        field: GenericDataclass[int]

    env.set('field', '{"x": 1}')
    s = ComplexSettings()
    assert s.field.x == 1

    env.set('field', '{"x": "a"}')
    with pytest.raises(ValidationError) as exc_info:
        ComplexSettings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'input': 'a',
            'loc': ('field', 'x'),
            'msg': 'Input should be a valid integer, unable to parse string as an integer',
            'type': 'int_parsing',
        }
    ]


def test_generic_basemodel(env):
    T = TypeVar('T')

    class GenericModel(BaseModel, Generic[T]):
        x: T

    class ComplexSettings(BaseSettings):
        field: GenericModel[int]

    env.set('field', '{"x": 1}')
    s = ComplexSettings()
    assert s.field.x == 1

    env.set('field', '{"x": "a"}')
    with pytest.raises(ValidationError) as exc_info:
        ComplexSettings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'input': 'a',
            'loc': ('field', 'x'),
            'msg': 'Input should be a valid integer, unable to parse string as an integer',
            'type': 'int_parsing',
        }
    ]


def test_annotated(env):
    T = TypeVar('T')

    class GenericModel(BaseModel, Generic[T]):
        x: T

    class ComplexSettings(BaseSettings):
        field: GenericModel[int]

    env.set('field', '{"x": 1}')
    s = ComplexSettings()
    assert s.field.x == 1

    env.set('field', '{"x": "a"}')
    with pytest.raises(ValidationError) as exc_info:
        ComplexSettings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'input': 'a',
            'loc': ('field', 'x'),
            'msg': 'Input should be a valid integer, unable to parse string as an integer',
            'type': 'int_parsing',
        }
    ]


def test_class_nested_model_default_partial_update(env):
    class NestedA(BaseModel):
        v0: bool
        v1: bool

    @pydantic_dataclasses.dataclass
    class NestedB:
        v0: bool
        v1: bool

    @dataclasses.dataclass
    class NestedC:
        v0: bool
        v1: bool

    class NestedD(BaseModel):
        v0: bool = False
        v1: bool = True

    class SettingsDefaultsA(BaseSettings, env_nested_delimiter='__', nested_model_default_partial_update=True):
        nested_a: NestedA = NestedA(v0=False, v1=True)
        nested_b: NestedB = NestedB(v0=False, v1=True)
        nested_d: NestedC = NestedC(v0=False, v1=True)
        nested_c: NestedD = NestedD()

    assert SettingsDefaultsA().model_dump() == {
        'nested_a': {'v0': False, 'v1': True},
        'nested_b': {'v0': False, 'v1': True},
        'nested_c': {'v0': False, 'v1': True},
        'nested_d': {'v0': False, 'v1': True},
    }
    assert SettingsDefaultsA().model_dump(exclude_unset=True) == {}

    env.set('NESTED_A__V0', 'True')
    env.set('NESTED_B__V0', 'True')
    assert SettingsDefaultsA().model_dump() == {
        'nested_a': {'v0': True, 'v1': True},
        'nested_b': {'v0': True, 'v1': True},
        'nested_c': {'v0': False, 'v1': True},
        'nested_d': {'v0': False, 'v1': True},
    }
    assert SettingsDefaultsA().model_dump(exclude_unset=True) == {
        'nested_a': {'v0': True, 'v1': True},
        'nested_b': {'v0': True, 'v1': True},
    }

    env.set('NESTED_C__V0', 'True')
    env.set('NESTED_D__V0', 'True')
    assert SettingsDefaultsA().model_dump() == {
        'nested_a': {'v0': True, 'v1': True},
        'nested_b': {'v0': True, 'v1': True},
        'nested_c': {'v0': True, 'v1': True},
        'nested_d': {'v0': True, 'v1': True},
    }
    assert SettingsDefaultsA().model_dump(exclude_unset=True) == {
        'nested_a': {'v0': True, 'v1': True},
        'nested_b': {'v0': True, 'v1': True},
        'nested_c': {'v0': True, 'v1': True},
        'nested_d': {'v0': True, 'v1': True},
    }


def test_init_kwargs_nested_model_default_partial_update(env):
    class DeepSubModel(BaseModel):
        v4: str

    class SubModel(BaseModel):
        v1: str
        v2: bytes
        v3: int
        deep: DeepSubModel

    class Settings(BaseSettings, env_nested_delimiter='__', nested_model_default_partial_update=True):
        v0: str
        sub_model: SubModel

        @classmethod
        def settings_customise_sources(
            cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings
        ):
            return env_settings, dotenv_settings, init_settings, file_secret_settings

    env.set('SUB_MODEL__DEEP__V4', 'override-v4')

    s_final = {'v0': '0', 'sub_model': {'v1': 'init-v1', 'v2': b'init-v2', 'v3': 3, 'deep': {'v4': 'override-v4'}}}

    s = Settings(v0='0', sub_model={'v1': 'init-v1', 'v2': b'init-v2', 'v3': 3, 'deep': {'v4': 'init-v4'}})
    assert s.model_dump() == s_final

    s = Settings(v0='0', sub_model=SubModel(v1='init-v1', v2=b'init-v2', v3=3, deep=DeepSubModel(v4='init-v4')))
    assert s.model_dump() == s_final

    s = Settings(v0='0', sub_model=SubModel(v1='init-v1', v2=b'init-v2', v3=3, deep={'v4': 'init-v4'}))
    assert s.model_dump() == s_final

    s = Settings(v0='0', sub_model={'v1': 'init-v1', 'v2': b'init-v2', 'v3': 3, 'deep': DeepSubModel(v4='init-v4')})
    assert s.model_dump() == s_final


def test_alias_resolution_init_source(env):
    class Example(BaseSettings):
        model_config = SettingsConfigDict(env_prefix='PREFIX')

        name: str
        last_name: str = Field(validation_alias=AliasChoices('PREFIX_LAST_NAME', 'PREFIX_SURNAME'))

    env.set('PREFIX_SURNAME', 'smith')
    assert Example(name='john', PREFIX_SURNAME='doe').model_dump() == {'name': 'john', 'last_name': 'doe'}

    class Settings(BaseSettings):
        NAME: str = Field(
            default='',
            validation_alias=AliasChoices('NAME', 'OLD_NAME'),
        )

        @model_validator(mode='before')
        def check_for_deprecated_attributes(cls, data: Any) -> Any:
            if isinstance(data, dict):
                old_keys = {k for k in data.keys() if k.startswith('OLD_')}
                assert not old_keys
            return data

    s = Settings(NAME='foo')
    s.model_dump() == {'NAME': 'foo'}

    with pytest.raises(ValidationError, match="Assertion failed, assert not {'OLD_NAME'}"):
        Settings(OLD_NAME='foo')


def test_init_kwargs_alias_resolution_deterministic():
    class Example(BaseSettings):
        name: str
        last_name: str = Field(validation_alias=AliasChoices('surname', 'last_name'))

    result = Example(name='john', surname='doe', last_name='smith').model_dump()

    assert result == {'name': 'john', 'last_name': 'doe'}


def test_alias_nested_model_default_partial_update():
    class SubModel(BaseModel):
        v1: str = 'default'
        v2: bytes = b'hello'
        v3: int

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            nested_model_default_partial_update=True, alias_generator=AliasGenerator(lambda s: s.replace('_', '-'))
        )

        v0: str = 'ok'
        sub_model: SubModel = SubModel(v1='top default', v3=33)

    assert Settings(**{'sub-model': {'v1': 'cli'}}).model_dump() == {
        'v0': 'ok',
        'sub_model': {'v1': 'cli', 'v2': b'hello', 'v3': 33},
    }


def test_env_str(env):
    class Settings(BaseSettings):
        apple: str = Field(None, validation_alias='BOOM')

    env.set('BOOM', 'hello')
    assert Settings().apple == 'hello'


def test_env_list(env):
    class Settings(BaseSettings):
        foobar: str = Field(validation_alias=AliasChoices('different1', 'different2'))

    env.set('different1', 'value 1')
    env.set('different2', 'value 2')
    s = Settings()
    assert s.foobar == 'value 1'


def test_env_list_field(env):
    class Settings(BaseSettings):
        foobar: str = Field(validation_alias='foobar_env_name')

    env.set('FOOBAR_ENV_NAME', 'env value')
    s = Settings()
    assert s.foobar == 'env value'


def test_env_list_last(env):
    class Settings(BaseSettings):
        foobar: str = Field(validation_alias=AliasChoices('different2'))

    env.set('different1', 'value 1')
    env.set('different2', 'value 2')
    s = Settings()
    assert s.foobar == 'value 2'


def test_env_inheritance_field(env):
    class SettingsParent(BaseSettings):
        foobar: str = Field('parent default', validation_alias='foobar_env')

    class SettingsChild(SettingsParent):
        foobar: str = 'child default'

    assert SettingsParent().foobar == 'parent default'

    assert SettingsChild().foobar == 'child default'
    assert SettingsChild(foobar='abc').foobar == 'abc'
    env.set('foobar_env', 'env value')
    assert SettingsParent().foobar == 'env value'
    assert SettingsChild().foobar == 'child default'
    assert SettingsChild(foobar='abc').foobar == 'abc'


def test_env_inheritance_config(env):
    env.set('foobar', 'foobar')
    env.set('prefix_foobar', 'prefix_foobar')

    env.set('foobar_parent_from_field', 'foobar_parent_from_field')
    env.set('prefix_foobar_parent_from_field', 'prefix_foobar_parent_from_field')

    env.set('foobar_parent_from_config', 'foobar_parent_from_config')
    env.set('foobar_child_from_config', 'foobar_child_from_config')

    env.set('foobar_child_from_field', 'foobar_child_from_field')

    # a. Child class config overrides prefix
    class Parent(BaseSettings):
        foobar: str = Field(None, validation_alias='foobar_parent_from_field')

        model_config = SettingsConfigDict(env_prefix='p_')

    class Child(Parent):
        model_config = SettingsConfigDict(env_prefix='prefix_')

    assert Child().foobar == 'foobar_parent_from_field'

    # b. Child class overrides field
    class Parent(BaseSettings):
        foobar: str = Field(None, validation_alias='foobar_parent_from_config')

    class Child(Parent):
        foobar: str = Field(None, validation_alias='foobar_child_from_config')

    assert Child().foobar == 'foobar_child_from_config'

    # . Child class overrides parent prefix and field
    class Parent(BaseSettings):
        foobar: str | None

        model_config = SettingsConfigDict(env_prefix='p_')

    class Child(Parent):
        foobar: str = Field(None, validation_alias='foobar_child_from_field')

        model_config = SettingsConfigDict(env_prefix='prefix_')

    assert Child().foobar == 'foobar_child_from_field'


def test_invalid_validation_alias(env):
    with pytest.raises(
        TypeError, match='Invalid `validation_alias` type. it should be `str`, `AliasChoices`, or `AliasPath`'
    ):

        class Settings(BaseSettings):
            foobar: str = Field(validation_alias=123)


def test_validation_aliases(env):
    class Settings(BaseSettings):
        foobar: str = Field('default value', validation_alias='foobar_alias')

    assert Settings().foobar == 'default value'
    assert Settings(foobar_alias='42').foobar == '42'
    env.set('foobar_alias', 'xxx')
    assert Settings().foobar == 'xxx'
    assert Settings(foobar_alias='42').foobar == '42'


def test_validation_aliases_alias_path(env):
    class Settings(BaseSettings):
        foobar: str = Field(validation_alias=AliasPath('foo', 'bar', 1))

    env.set('foo', '{"bar": ["val0", "val1"]}')
    assert Settings().foobar == 'val1'


def test_validation_aliases_alias_choices(env):
    class Settings(BaseSettings):
        foobar: str = Field(validation_alias=AliasChoices('foo', AliasPath('foo1', 'bar', 1), AliasPath('bar', 2)))

    env.set('foo', 'val1')
    assert Settings().foobar == 'val1'

    env.pop('foo')
    env.set('foo1', '{"bar": ["val0", "val2"]}')
    assert Settings().foobar == 'val2'

    env.pop('foo1')
    env.set('bar', '["val1", "val2", "val3"]')
    assert Settings().foobar == 'val3'


def test_validation_alias_with_env_prefix(env):
    class Settings(BaseSettings):
        foobar: str = Field(validation_alias='foo')

        model_config = SettingsConfigDict(env_prefix='p_')

    env.set('p_foo', 'bar')
    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {'type': 'missing', 'loc': ('foo',), 'msg': 'Field required', 'input': {}}
    ]

    env.set('foo', 'bar')
    assert Settings().foobar == 'bar'


def test_case_sensitive(monkeypatch):
    class Settings(BaseSettings):
        foo: str

        model_config = SettingsConfigDict(case_sensitive=True)

    # Need to patch os.environ to get build to work on Windows, where os.environ is case insensitive
    monkeypatch.setattr(os, 'environ', value={'Foo': 'foo'})
    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {'type': 'missing', 'loc': ('foo',), 'msg': 'Field required', 'input': {}}
    ]


@pytest.mark.parametrize('env_nested_delimiter', [None, ''])
def test_case_sensitive_no_nested_delimiter(monkeypatch, env_nested_delimiter):
    class Subsettings(BaseSettings):
        foo: str

    class Settings(BaseSettings):
        subsettings: Subsettings

        model_config = SettingsConfigDict(case_sensitive=True, env_nested_delimiter=env_nested_delimiter)

    # Need to patch os.environ to get build to work on Windows, where os.environ is case insensitive
    monkeypatch.setattr(os, 'environ', value={'subsettingsNonefoo': '1'})
    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {'type': 'missing', 'loc': ('subsettings',), 'msg': 'Field required', 'input': {}}
    ]


def test_nested_dataclass(env):
    @pydantic_dataclasses.dataclass
    class DeepNestedDataclass:
        boo: int
        rar: str

    @pydantic_dataclasses.dataclass
    class MyDataclass:
        foo: int
        bar: str
        deep: DeepNestedDataclass

    class Settings(BaseSettings, env_nested_delimiter='__'):
        n: MyDataclass

    env.set('N', '{"foo": 123, "bar": "bar value"}')
    env.set('N__DEEP', '{"boo": 1, "rar": "eek"}')
    s = Settings()
    assert isinstance(s.n, MyDataclass)
    assert s.n.foo == 123
    assert s.n.bar == 'bar value'


def test_nested_vanilla_dataclass(env):
    @dataclasses.dataclass
    class MyDataclass:
        value: str

    class NestedSettings(BaseSettings, MyDataclass):
        pass

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__')

        sub: NestedSettings

    env.set('SUB__VALUE', 'something')
    s = Settings()
    assert s.sub.value == 'something'


def test_env_takes_precedence(env):
    class Settings(BaseSettings):
        foo: int
        bar: str

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return env_settings, init_settings

    env.set('BAR', 'env setting')

    s = Settings(foo='123', bar='argument')
    assert s.foo == 123
    assert s.bar == 'env setting'


def test_config_file_settings_nornir(env):
    """
    See https://github.com/pydantic/pydantic/pull/341#issuecomment-450378771
    """

    def nornir_settings_source() -> dict[str, Any]:
        return {'param_a': 'config a', 'param_b': 'config b', 'param_c': 'config c'}

    class Settings(BaseSettings):
        param_a: str
        param_b: str
        param_c: str

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return env_settings, init_settings, nornir_settings_source

    env.set('PARAM_C', 'env setting c')

    s = Settings(param_b='argument b', param_c='argument c')
    assert s.param_a == 'config a'
    assert s.param_b == 'argument b'
    assert s.param_c == 'env setting c'


def test_env_union_with_complex_subfields_parses_json(env):
    class A(BaseModel):
        a: str

    class B(BaseModel):
        b: int

    class Settings(BaseSettings):
        content: A | B | int

    env.set('content', '{"a": "test"}')
    s = Settings()
    assert s.content == A(a='test')


def test_env_union_with_complex_subfields_parses_plain_if_json_fails(env):
    class A(BaseModel):
        a: str

    class B(BaseModel):
        b: int

    class Settings(BaseSettings):
        content: A | B | datetime

    env.set('content', '{"a": "test"}')
    s = Settings()
    assert s.content == A(a='test')

    env.set('content', '2020-07-05T00:00:00Z')
    s = Settings()
    assert s.content == datetime(2020, 7, 5, 0, 0, tzinfo=timezone.utc)


def test_env_union_without_complex_subfields_does_not_parse_json(env):
    class Settings(BaseSettings):
        content: datetime | str

    env.set('content', '2020-07-05T00:00:00Z')
    s = Settings()
    assert s.content == '2020-07-05T00:00:00Z'


test_env_file = """\
# this is a comment
A=good string
# another one, followed by whitespace

b='better string'
c="best string"
"""


def test_env_file_config(env, tmp_path):
    p = tmp_path / '.env'
    p.write_text(test_env_file)

    class Settings(BaseSettings):
        a: str
        b: str
        c: str

        model_config = SettingsConfigDict(env_file=p)

    env.set('A', 'overridden var')

    s = Settings()
    assert s.a == 'overridden var'
    assert s.b == 'better string'
    assert s.c == 'best string'


prefix_test_env_file = """\
# this is a comment
prefix_A=good string
# another one, followed by whitespace

prefix_b='better string'
prefix_c="best string"
"""


def test_env_file_with_env_prefix(env, tmp_path):
    p = tmp_path / '.env'
    p.write_text(prefix_test_env_file)

    class Settings(BaseSettings):
        a: str
        b: str
        c: str

        model_config = SettingsConfigDict(env_file=p, env_prefix='prefix_')

    env.set('prefix_A', 'overridden var')

    s = Settings()
    assert s.a == 'overridden var'
    assert s.b == 'better string'
    assert s.c == 'best string'


prefix_test_env_invalid_file = """\
# this is a comment
prefix_A=good string
# another one, followed by whitespace

prefix_b='better string'
prefix_c="best string"
f="random value"
"""


def test_env_file_with_env_prefix_invalid(tmp_path):
    p = tmp_path / '.env'
    p.write_text(prefix_test_env_invalid_file)

    class Settings(BaseSettings):
        a: str
        b: str
        c: str

        model_config = SettingsConfigDict(env_file=p, env_prefix='prefix_')

    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {'type': 'extra_forbidden', 'loc': ('f',), 'msg': 'Extra inputs are not permitted', 'input': 'random value'}
    ]


def test_ignore_env_file_with_env_prefix_invalid(tmp_path):
    p = tmp_path / '.env'
    p.write_text(prefix_test_env_invalid_file)

    class Settings(BaseSettings):
        a: str
        b: str
        c: str

        model_config = SettingsConfigDict(env_file=p, env_prefix='prefix_', extra='ignore')

    s = Settings()

    assert s.a == 'good string'
    assert s.b == 'better string'
    assert s.c == 'best string'


def test_env_file_config_case_sensitive(tmp_path):
    p = tmp_path / '.env'
    p.write_text(test_env_file)

    class Settings(BaseSettings):
        a: str
        b: str
        c: str

        model_config = SettingsConfigDict(env_file=p, case_sensitive=True, extra='ignore')

    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'type': 'missing',
            'loc': ('a',),
            'msg': 'Field required',
            'input': {'b': 'better string', 'c': 'best string', 'A': 'good string'},
        }
    ]


def test_env_file_export(env, tmp_path):
    p = tmp_path / '.env'
    p.write_text(
        """\
export A='good string'
export B=better-string
export C="best string"
"""
    )

    class Settings(BaseSettings):
        a: str
        b: str
        c: str

        model_config = SettingsConfigDict(env_file=p)

    env.set('A', 'overridden var')

    s = Settings()
    assert s.a == 'overridden var'
    assert s.b == 'better-string'
    assert s.c == 'best string'


def test_env_file_export_validation_alias(env, tmp_path):
    p = tmp_path / '.env'
    p.write_text("""export a='{"b": ["1", "2"]}'""")

    class Settings(BaseSettings):
        a: str = Field(validation_alias=AliasChoices(AliasPath('a', 'b', 1)))

        model_config = SettingsConfigDict(env_file=p)

    s = Settings()
    assert s.a == '2'


def test_env_file_config_custom_encoding(tmp_path):
    p = tmp_path / '.env'
    p.write_text('pika=p!±@', encoding='latin-1')

    class Settings(BaseSettings):
        pika: str

        model_config = SettingsConfigDict(env_file=p, env_file_encoding='latin-1')

    s = Settings()
    assert s.pika == 'p!±@'


@pytest.fixture
def home_tmp(tmp_path, env):
    env.set('HOME', str(tmp_path))
    env.set('USERPROFILE', str(tmp_path))
    env.set('HOMEPATH', str(tmp_path))

    tmp_filename = f'{uuid.uuid4()}.env'
    home_tmp_path = tmp_path / tmp_filename
    yield home_tmp_path, tmp_filename
    home_tmp_path.unlink()


def test_env_file_home_directory(home_tmp):
    home_tmp_path, tmp_filename = home_tmp
    home_tmp_path.write_text('pika=baz')

    class Settings(BaseSettings):
        pika: str

        model_config = SettingsConfigDict(env_file=f'~/{tmp_filename}')

    assert Settings().pika == 'baz'


def test_env_file_none(tmp_path):
    p = tmp_path / '.env'
    p.write_text('a')

    class Settings(BaseSettings):
        a: str = 'xxx'

    s = Settings(_env_file=p)
    assert s.a == 'xxx'


def test_env_file_override_file(tmp_path):
    p1 = tmp_path / '.env'
    p1.write_text(test_env_file)
    p2 = tmp_path / '.env.prod'
    p2.write_text('A="new string"')

    class Settings(BaseSettings):
        a: str

        model_config = SettingsConfigDict(env_file=str(p1))

    s = Settings(_env_file=p2)
    assert s.a == 'new string'


def test_env_file_override_none(tmp_path):
    p = tmp_path / '.env'
    p.write_text(test_env_file)

    class Settings(BaseSettings):
        a: str | None = None

        model_config = SettingsConfigDict(env_file=p)

    s = Settings(_env_file=None)
    assert s.a is None


def test_env_file_not_a_file(env):
    class Settings(BaseSettings):
        a: str = None

    env.set('A', 'ignore non-file')
    s = Settings(_env_file='tests/')
    assert s.a == 'ignore non-file'


def test_read_env_file_case_sensitive(tmp_path):
    p = tmp_path / '.env'
    p.write_text('a="test"\nB=123')

    assert DotEnvSettingsSource._static_read_env_file(p) == {'a': 'test', 'b': '123'}
    assert DotEnvSettingsSource._static_read_env_file(p, case_sensitive=True) == {'a': 'test', 'B': '123'}


def test_read_env_file_syntax_wrong(tmp_path):
    p = tmp_path / '.env'
    p.write_text('NOT_AN_ASSIGNMENT')

    assert DotEnvSettingsSource._static_read_env_file(p, case_sensitive=True) == {'NOT_AN_ASSIGNMENT': None}


def test_env_file_example(tmp_path):
    p = tmp_path / '.env'
    p.write_text(
        """\
# ignore comment
ENVIRONMENT="production"
REDIS_ADDRESS=localhost:6379
MEANING_OF_LIFE=42
MY_VAR='Hello world'
"""
    )

    class Settings(BaseSettings):
        environment: str
        redis_address: str
        meaning_of_life: int
        my_var: str

    s = Settings(_env_file=str(p))
    assert s.model_dump() == {
        'environment': 'production',
        'redis_address': 'localhost:6379',
        'meaning_of_life': 42,
        'my_var': 'Hello world',
    }


def test_env_file_custom_encoding(tmp_path):
    p = tmp_path / '.env'
    p.write_text('pika=p!±@', encoding='latin-1')

    class Settings(BaseSettings):
        pika: str

    with pytest.raises(UnicodeDecodeError):
        Settings(_env_file=str(p))

    s = Settings(_env_file=str(p), _env_file_encoding='latin-1')
    assert s.model_dump() == {'pika': 'p!±@'}


test_default_env_file = """\
debug_mode=true
host=localhost
Port=8000
"""

test_prod_env_file = """\
debug_mode=false
host=https://example.com/services
"""


def test_multiple_env_file(tmp_path):
    base_env = tmp_path / '.env'
    base_env.write_text(test_default_env_file)
    prod_env = tmp_path / '.env.prod'
    prod_env.write_text(test_prod_env_file)

    class Settings(BaseSettings):
        debug_mode: bool
        host: str
        port: int

        model_config = SettingsConfigDict(env_file=[base_env, prod_env])

    s = Settings()
    assert s.debug_mode is False
    assert s.host == 'https://example.com/services'
    assert s.port == 8000


def test_model_env_file_override_model_config(tmp_path):
    base_env = tmp_path / '.env'
    base_env.write_text(test_default_env_file)
    prod_env = tmp_path / '.env.prod'
    prod_env.write_text(test_prod_env_file)

    class Settings(BaseSettings):
        debug_mode: bool
        host: str
        port: int

        model_config = SettingsConfigDict(env_file=prod_env)

    s = Settings(_env_file=base_env)
    assert s.debug_mode is True
    assert s.host == 'localhost'
    assert s.port == 8000


def test_multiple_env_file_encoding(tmp_path):
    base_env = tmp_path / '.env'
    base_env.write_text('pika=p!±@', encoding='latin-1')
    prod_env = tmp_path / '.env.prod'
    prod_env.write_text('pika=chu!±@', encoding='latin-1')

    class Settings(BaseSettings):
        pika: str

    s = Settings(_env_file=[base_env, prod_env], _env_file_encoding='latin-1')
    assert s.pika == 'chu!±@'


def test_read_dotenv_vars(tmp_path):
    base_env = tmp_path / '.env'
    base_env.write_text(test_default_env_file)
    prod_env = tmp_path / '.env.prod'
    prod_env.write_text(test_prod_env_file)

    source = DotEnvSettingsSource(
        BaseSettings(), env_file=[base_env, prod_env], env_file_encoding='utf8', case_sensitive=False
    )
    assert source._read_env_files() == {
        'debug_mode': 'false',
        'host': 'https://example.com/services',
        'port': '8000',
    }

    source = DotEnvSettingsSource(
        BaseSettings(), env_file=[base_env, prod_env], env_file_encoding='utf8', case_sensitive=True
    )
    assert source._read_env_files() == {
        'debug_mode': 'false',
        'host': 'https://example.com/services',
        'Port': '8000',
    }


def test_read_dotenv_vars_when_env_file_is_none():
    assert (
        DotEnvSettingsSource(
            BaseSettings(), env_file=None, env_file_encoding=None, case_sensitive=False
        )._read_env_files()
        == {}
    )


def test_dotenvsource_override(env):
    class StdinDotEnvSettingsSource(DotEnvSettingsSource):
        @override
        def _read_env_file(self, file_path: Path) -> dict[str, str]:
            assert str(file_path) == '-'
            return {'foo': 'stdin_foo', 'bar': 'stdin_bar'}

        @override
        def _read_env_files(self) -> dict[str, str]:
            return self._read_env_file(Path('-'))

    source = StdinDotEnvSettingsSource(BaseSettings())
    assert source._read_env_files() == {'foo': 'stdin_foo', 'bar': 'stdin_bar'}


# test that calling read_env_file issues a DeprecationWarning
# TODO: remove this test once read_env_file is removed
def test_read_env_file_deprecation(tmp_path):
    from pydantic_settings.sources import read_env_file

    base_env = tmp_path / '.env'
    base_env.write_text(test_default_env_file)

    with pytest.deprecated_call():
        assert read_env_file(base_env) == {
            'debug_mode': 'true',
            'host': 'localhost',
            'port': '8000',
        }


def test_alias_set(env):
    class Settings(BaseSettings):
        foo: str = Field('default foo', validation_alias='foo_env')
        bar: str = 'bar default'

    assert Settings.model_fields['bar'].alias is None
    assert Settings.model_fields['bar'].validation_alias is None
    assert Settings.model_fields['foo'].alias is None
    assert Settings.model_fields['foo'].validation_alias == 'foo_env'

    class SubSettings(Settings):
        spam: str = 'spam default'

    assert SubSettings.model_fields['bar'].alias is None
    assert SubSettings.model_fields['bar'].validation_alias is None
    assert SubSettings.model_fields['foo'].alias is None
    assert SubSettings.model_fields['foo'].validation_alias == 'foo_env'

    assert SubSettings().model_dump() == {'foo': 'default foo', 'bar': 'bar default', 'spam': 'spam default'}
    env.set('foo_env', 'fff')
    assert SubSettings().model_dump() == {'foo': 'fff', 'bar': 'bar default', 'spam': 'spam default'}
    env.set('bar', 'bbb')
    assert SubSettings().model_dump() == {'foo': 'fff', 'bar': 'bbb', 'spam': 'spam default'}
    env.set('spam', 'sss')
    assert SubSettings().model_dump() == {'foo': 'fff', 'bar': 'bbb', 'spam': 'sss'}


def test_prefix_on_parent(env):
    class MyBaseSettings(BaseSettings):
        var: str = 'old'

    class MySubSettings(MyBaseSettings):
        model_config = SettingsConfigDict(env_prefix='PREFIX_')

    assert MyBaseSettings().model_dump() == {'var': 'old'}
    assert MySubSettings().model_dump() == {'var': 'old'}
    env.set('PREFIX_VAR', 'new')
    assert MyBaseSettings().model_dump() == {'var': 'old'}
    assert MySubSettings().model_dump() == {'var': 'new'}


def test_secrets_path(tmp_path):
    p = tmp_path / 'foo'
    p.write_text('foo_secret_value_str')

    class Settings(BaseSettings):
        foo: str

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    assert Settings().model_dump() == {'foo': 'foo_secret_value_str'}


def test_secrets_path_multiple(tmp_path):
    d1 = tmp_path / 'dir1'
    d2 = tmp_path / 'dir2'
    d1.mkdir()
    d2.mkdir()
    (d1 / 'foo1').write_text('foo1_dir1_secret_value_str')
    (d1 / 'foo2').write_text('foo2_dir1_secret_value_str')
    (d2 / 'foo2').write_text('foo2_dir2_secret_value_str')
    (d2 / 'foo3').write_text('foo3_dir2_secret_value_str')

    class Settings(BaseSettings):
        foo1: str
        foo2: str
        foo3: str

    assert Settings(_secrets_dir=(d1, d2)).model_dump() == {
        'foo1': 'foo1_dir1_secret_value_str',
        'foo2': 'foo2_dir2_secret_value_str',  # dir2 takes priority
        'foo3': 'foo3_dir2_secret_value_str',
    }
    assert Settings(_secrets_dir=(d2, d1)).model_dump() == {
        'foo1': 'foo1_dir1_secret_value_str',
        'foo2': 'foo2_dir1_secret_value_str',  # dir1 takes priority
        'foo3': 'foo3_dir2_secret_value_str',
    }


def test_secrets_path_with_validation_alias(tmp_path):
    p = tmp_path / 'foo'
    p.write_text('{"bar": ["test"]}')

    class Settings(BaseSettings):
        foo: str = Field(validation_alias=AliasChoices(AliasPath('foo', 'bar', 0)))

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    assert Settings().model_dump() == {'foo': 'test'}


def test_secrets_case_sensitive(tmp_path):
    (tmp_path / 'SECRET_VAR').write_text('foo_env_value_str')

    class Settings(BaseSettings):
        secret_var: str | None = None

        model_config = SettingsConfigDict(secrets_dir=tmp_path, case_sensitive=True)

    assert Settings().model_dump() == {'secret_var': None}


def test_secrets_case_insensitive(tmp_path):
    (tmp_path / 'SECRET_VAR').write_text('foo_env_value_str')

    class Settings(BaseSettings):
        secret_var: str | None

        model_config = SettingsConfigDict(secrets_dir=tmp_path, case_sensitive=False)

    settings = Settings().model_dump()
    assert settings == {'secret_var': 'foo_env_value_str'}


def test_secrets_path_url(tmp_path):
    (tmp_path / 'foo').write_text('http://www.example.com')
    (tmp_path / 'bar').write_text('snap')

    class Settings(BaseSettings):
        foo: HttpUrl
        bar: SecretStr

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    settings = Settings()
    assert str(settings.foo) == 'http://www.example.com/'
    assert settings.bar == SecretStr('snap')


def test_secrets_path_json(tmp_path):
    p = tmp_path / 'foo'
    p.write_text('{"a": "b"}')

    class Settings(BaseSettings):
        foo: dict[str, str]

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    assert Settings().model_dump() == {'foo': {'a': 'b'}}


def test_secrets_nested_optional_json(tmp_path):
    p = tmp_path / 'foo'
    p.write_text('{"a": 10}')

    class Foo(BaseModel):
        a: int

    class Settings(BaseSettings):
        foo: Foo | None = None

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    assert Settings().model_dump() == {'foo': {'a': 10}}


def test_secrets_path_invalid_json(tmp_path):
    p = tmp_path / 'foo'
    p.write_text('{"a": "b"')

    class Settings(BaseSettings):
        foo: dict[str, str]

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    with pytest.raises(SettingsError, match='error parsing value for field "foo" from source "SecretsSettingsSource"'):
        Settings()


def test_secrets_missing(tmp_path):
    class Settings(BaseSettings):
        foo: str
        bar: list[str]

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    with pytest.raises(ValidationError) as exc_info:
        Settings()

    assert exc_info.value.errors(include_url=False) == [
        {'type': 'missing', 'loc': ('foo',), 'msg': 'Field required', 'input': {}},
        {'input': {}, 'loc': ('bar',), 'msg': 'Field required', 'type': 'missing'},
    ]


def test_secrets_invalid_secrets_dir(tmp_path):
    p1 = tmp_path / 'foo'
    p1.write_text('foo_secret_value_str')

    class Settings(BaseSettings):
        foo: str

        model_config = SettingsConfigDict(secrets_dir=p1)

    with pytest.raises(SettingsError, match='secrets_dir must reference a directory, not a file'):
        Settings()


def test_secrets_invalid_secrets_dir_multiple_all(tmp_path):
    class Settings(BaseSettings):
        foo: str

    (d1 := tmp_path / 'dir1').write_text('')
    (d2 := tmp_path / 'dir2').write_text('')

    with pytest.raises(SettingsError, match='secrets_dir must reference a directory, not a file'):
        Settings(_secrets_dir=[d1, d2])


def test_secrets_invalid_secrets_dir_multiple_one(tmp_path):
    class Settings(BaseSettings):
        foo: str

    (d1 := tmp_path / 'dir1').mkdir()
    (d2 := tmp_path / 'dir2').write_text('')

    with pytest.raises(SettingsError, match='secrets_dir must reference a directory, not a file'):
        Settings(_secrets_dir=[d1, d2])


@pytest.mark.skipif(sys.platform.startswith('win'), reason='windows paths break regex')
def test_secrets_missing_location(tmp_path):
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(secrets_dir=tmp_path / 'does_not_exist')

    with pytest.warns(UserWarning, match=f'directory "{tmp_path}/does_not_exist" does not exist'):
        Settings()


@pytest.mark.skipif(sys.platform.startswith('win'), reason='windows paths break regex')
def test_secrets_missing_location_multiple_all(tmp_path):
    class Settings(BaseSettings):
        foo: str | None = None

    with pytest.warns() as record:
        Settings(_secrets_dir=[tmp_path / 'dir1', tmp_path / 'dir2'])

    assert len(record) == 2
    assert record[0].category is UserWarning and record[1].category is UserWarning
    assert str(record[0].message) == f'directory "{tmp_path}/dir1" does not exist'
    assert str(record[1].message) == f'directory "{tmp_path}/dir2" does not exist'


@pytest.mark.skipif(sys.platform.startswith('win'), reason='windows paths break regex')
def test_secrets_missing_location_multiple_one(tmp_path):
    class Settings(BaseSettings):
        foo: str | None = None

    (d1 := tmp_path / 'dir1').mkdir()
    (d1 / 'foo').write_text('secret_value')

    with pytest.warns(UserWarning, match=f'directory "{tmp_path}/dir2" does not exist'):
        conf = Settings(_secrets_dir=[d1, tmp_path / 'dir2'])

    assert conf.foo == 'secret_value'  # value obtained from first directory


@pytest.mark.skipif(sys.platform.startswith('win'), reason='windows paths break regex')
def test_secrets_file_is_a_directory(tmp_path):
    p1 = tmp_path / 'foo'
    p1.mkdir()

    class Settings(BaseSettings):
        foo: str | None = None

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    with pytest.warns(
        UserWarning, match=f'attempted to load secret file "{tmp_path}/foo" but found a directory instead'
    ):
        Settings()


@pytest.mark.skipif(sys.platform.startswith('win'), reason='windows paths break regex')
def test_secrets_file_is_a_directory_multiple_all(tmp_path):
    class Settings(BaseSettings):
        foo: str | None = None

    (d1 := tmp_path / 'dir1').mkdir()
    (d2 := tmp_path / 'dir2').mkdir()
    (d1 / 'foo').mkdir()
    (d2 / 'foo').mkdir()

    with pytest.warns() as record:
        Settings(_secrets_dir=[d1, d2])

    assert len(record) == 2
    assert record[0].category is UserWarning and record[1].category is UserWarning
    # warnings are emitted in reverse order
    assert str(record[0].message) == f'attempted to load secret file "{d2}/foo" but found a directory instead.'
    assert str(record[1].message) == f'attempted to load secret file "{d1}/foo" but found a directory instead.'


@pytest.mark.skipif(sys.platform.startswith('win'), reason='windows paths break regex')
def test_secrets_file_is_a_directory_multiple_one(tmp_path):
    class Settings(BaseSettings):
        foo: str | None = None

    (d1 := tmp_path / 'dir1').mkdir()
    (d2 := tmp_path / 'dir2').mkdir()
    (d1 / 'foo').write_text('secret_value')
    (d2 / 'foo').mkdir()

    with pytest.warns(UserWarning, match=f'attempted to load secret file "{d2}/foo" but found a directory instead.'):
        conf = Settings(_secrets_dir=[d1, d2])

    assert conf.foo == 'secret_value'  # value obtained from first directory


def test_secrets_dotenv_precedence(tmp_path):
    s = tmp_path / 'foo'
    s.write_text('foo_secret_value_str')

    e = tmp_path / '.env'
    e.write_text('foo=foo_env_value_str')

    class Settings(BaseSettings):
        foo: str

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

    assert Settings(_env_file=e).model_dump() == {'foo': 'foo_env_value_str'}


def test_external_settings_sources_precedence(env):
    def external_source_0() -> dict[str, str]:
        return {'apple': 'value 0', 'banana': 'value 2'}

    def external_source_1() -> dict[str, str]:
        return {'apple': 'value 1', 'raspberry': 'value 3'}

    class Settings(BaseSettings):
        apple: str
        banana: str
        raspberry: str

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (
                init_settings,
                env_settings,
                dotenv_settings,
                file_secret_settings,
                external_source_0,
                external_source_1,
            )

    env.set('banana', 'value 1')
    assert Settings().model_dump() == {'apple': 'value 0', 'banana': 'value 1', 'raspberry': 'value 3'}


def test_external_settings_sources_filter_env_vars():
    vault_storage = {'user:password': {'apple': 'value 0', 'banana': 'value 2'}}

    class VaultSettingsSource(PydanticBaseSettingsSource):
        def __init__(self, settings_cls: type[BaseSettings], user: str, password: str):
            self.user = user
            self.password = password
            super().__init__(settings_cls)

        def get_field_value(self, field: FieldInfo, field_name: str) -> Any:
            pass

        def __call__(self) -> dict[str, str]:
            vault_vars = vault_storage[f'{self.user}:{self.password}']
            return {
                field_name: vault_vars[field_name]
                for field_name in self.settings_cls.model_fields.keys()
                if field_name in vault_vars
            }

    class Settings(BaseSettings):
        apple: str
        banana: str

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (
                init_settings,
                env_settings,
                dotenv_settings,
                file_secret_settings,
                VaultSettingsSource(settings_cls, user='user', password='password'),
            )

    assert Settings().model_dump() == {'apple': 'value 0', 'banana': 'value 2'}


def test_customise_sources_empty():
    class Settings(BaseSettings):
        apple: str = 'default'
        banana: str = 'default'

        @classmethod
        def settings_customise_sources(cls, *args, **kwargs):
            return ()

    assert Settings().model_dump() == {'apple': 'default', 'banana': 'default'}
    assert Settings(apple='xxx').model_dump() == {'apple': 'default', 'banana': 'default'}


def test_builtins_settings_source_repr():
    assert (
        repr(DefaultSettingsSource(BaseSettings, nested_model_default_partial_update=True))
        == 'DefaultSettingsSource(nested_model_default_partial_update=True)'
    )
    assert (
        repr(InitSettingsSource(BaseSettings, init_kwargs={'apple': 'value 0', 'banana': 'value 1'}))
        == "InitSettingsSource(init_kwargs={'apple': 'value 0', 'banana': 'value 1'})"
    )
    assert (
        repr(EnvSettingsSource(BaseSettings, env_nested_delimiter='__'))
        == "EnvSettingsSource(env_nested_delimiter='__', env_prefix_len=0)"
    )
    assert repr(DotEnvSettingsSource(BaseSettings, env_file='.env', env_file_encoding='utf-8')) == (
        "DotEnvSettingsSource(env_file='.env', env_file_encoding='utf-8', env_nested_delimiter=None, env_prefix_len=0)"
    )
    assert (
        repr(SecretsSettingsSource(BaseSettings, secrets_dir='/secrets'))
        == "SecretsSettingsSource(secrets_dir='/secrets')"
    )


def _parse_custom_dict(value: str) -> Callable[[str], dict[int, str]]:
    """A custom parsing function passed into env parsing test."""
    res = {}
    for part in value.split(','):
        k, v = part.split('=')
        res[int(k)] = v
    return res


class CustomEnvSettingsSource(EnvSettingsSource):
    def prepare_field_value(self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool) -> Any:
        if not value:
            return None

        return _parse_custom_dict(value)


def test_env_setting_source_custom_env_parse(env):
    class Settings(BaseSettings):
        top: dict[int, str]

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (CustomEnvSettingsSource(settings_cls),)

    with pytest.raises(ValidationError):
        Settings()
    env.set('top', '1=apple,2=banana')
    s = Settings()
    assert s.top == {1: 'apple', 2: 'banana'}


class BadCustomEnvSettingsSource(EnvSettingsSource):
    def prepare_field_value(self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool) -> Any:
        """A custom parsing function passed into env parsing test."""
        return int(value)


def test_env_settings_source_custom_env_parse_is_bad(env):
    class Settings(BaseSettings):
        top: dict[int, str]

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (BadCustomEnvSettingsSource(settings_cls),)

    env.set('top', '1=apple,2=banana')
    with pytest.raises(
        SettingsError, match='error parsing value for field "top" from source "BadCustomEnvSettingsSource"'
    ):
        Settings()


class CustomSecretsSettingsSource(SecretsSettingsSource):
    def prepare_field_value(self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool) -> Any:
        if not value:
            return None

        return _parse_custom_dict(value)


def test_secret_settings_source_custom_env_parse(tmp_path):
    p = tmp_path / 'top'
    p.write_text('1=apple,2=banana')

    class Settings(BaseSettings):
        top: dict[int, str]

        model_config = SettingsConfigDict(secrets_dir=tmp_path)

        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (CustomSecretsSettingsSource(settings_cls, tmp_path),)

    s = Settings()
    assert s.top == {1: 'apple', 2: 'banana'}


class BadCustomSettingsSource(EnvSettingsSource):
    def get_field_value(self, field: FieldInfo, field_name: str) -> Any:
        raise ValueError('Error')


def test_custom_source_get_field_value_error(env):
    class Settings(BaseSettings):
        top: dict[int, str]

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (BadCustomSettingsSource(settings_cls),)

    with pytest.raises(
        SettingsError, match='error getting value for field "top" from source "BadCustomSettingsSource"'
    ):
        Settings()


def test_nested_env_complex_values(env):
    class SubSubModel(BaseSettings):
        dvals: dict

    class SubModel(BaseSettings):
        vals: list[str]
        sub_sub_model: SubSubModel

    class Cfg(BaseSettings):
        sub_model: SubModel

        model_config = SettingsConfigDict(env_prefix='cfg_', env_nested_delimiter='__')

    env.set('cfg_sub_model__vals', '["one", "two"]')
    env.set('cfg_sub_model__sub_sub_model__dvals', '{"three": 4}')

    assert Cfg().model_dump() == {'sub_model': {'vals': ['one', 'two'], 'sub_sub_model': {'dvals': {'three': 4}}}}

    env.set('cfg_sub_model__vals', 'invalid')
    with pytest.raises(
        SettingsError, match='error parsing value for field "sub_model" from source "EnvSettingsSource"'
    ):
        Cfg()


def test_nested_env_nonexisting_field(env):
    class SubModel(BaseSettings):
        vals: list[str]

    class Cfg(BaseSettings):
        sub_model: SubModel

        model_config = SettingsConfigDict(env_prefix='cfg_', env_nested_delimiter='__')

    env.set('cfg_sub_model__foo_vals', '[]')
    with pytest.raises(ValidationError):
        Cfg()


def test_nested_env_nonexisting_field_deep(env):
    class SubModel(BaseSettings):
        vals: list[str]

    class Cfg(BaseSettings):
        sub_model: SubModel

        model_config = SettingsConfigDict(env_prefix='cfg_', env_nested_delimiter='__')

    env.set('cfg_sub_model__vals__foo__bar__vals', '[]')
    with pytest.raises(ValidationError):
        Cfg()


def test_nested_env_union_complex_values(env):
    class SubModel(BaseSettings):
        vals: list[str] | dict[str, str]

    class Cfg(BaseSettings):
        sub_model: SubModel

        model_config = SettingsConfigDict(env_prefix='cfg_', env_nested_delimiter='__')

    env.set('cfg_sub_model__vals', '["one", "two"]')
    assert Cfg().model_dump() == {'sub_model': {'vals': ['one', 'two']}}

    env.set('cfg_sub_model__vals', '{"three": "four"}')
    assert Cfg().model_dump() == {'sub_model': {'vals': {'three': 'four'}}}

    env.set('cfg_sub_model__vals', 'stringval')
    with pytest.raises(ValidationError):
        Cfg()

    env.set('cfg_sub_model__vals', '{"invalid": dict}')
    with pytest.raises(ValidationError):
        Cfg()


def test_discriminated_union_with_callable_discriminator(env):
    class A(BaseModel):
        x: Literal['a'] = 'a'
        y: str

    class B(BaseModel):
        x: Literal['b'] = 'b'
        z: str

    def get_discriminator_value(v: Any) -> Hashable:
        if isinstance(v, dict):
            v0 = v.get('x')
        else:
            v0 = getattr(v, 'x', None)

        if v0 == 'a':
            return 'a'
        elif v0 == 'b':
            return 'b'
        else:
            return None

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__')

        # Discriminated union using a callable discriminator.
        a_or_b: Annotated[Annotated[A, Tag('a')] | Annotated[B, Tag('b')], Discriminator(get_discriminator_value)]

    # Set up environment so that the discriminator is 'a'.
    env.set('a_or_b__x', 'a')
    env.set('a_or_b__y', 'foo')

    s = Settings()

    assert s.a_or_b.x == 'a'
    assert s.a_or_b.y == 'foo'


def test_json_field_with_discriminated_union(env):
    class A(BaseModel):
        x: Literal['a'] = 'a'

    class B(BaseModel):
        x: Literal['b'] = 'b'

    A_OR_B = Annotated[A | B, Field(discriminator='x')]

    class Settings(BaseSettings):
        a_or_b: Json[A_OR_B] | None = None

    # Set up environment so that the discriminator is 'a'.
    env.set('a_or_b', '{"x": "a"}')

    s = Settings()

    assert s.a_or_b.x == 'a'


def test_nested_model_case_insensitive(env):
    class SubSubSub(BaseModel):
        VaL3: str
        val4: str = Field(validation_alias='VAL4')

    class SubSub(BaseModel):
        Val2: str
        SUB_sub_SuB: SubSubSub

    class Sub(BaseModel):
        VAL1: str
        SUB_sub: SubSub

    class Settings(BaseSettings):
        nested: Sub

        model_config = SettingsConfigDict(env_nested_delimiter='__')

    env.set('nested', '{"val1": "v1", "sub_SUB": {"VAL2": "v2", "sub_SUB_sUb": {"vAl3": "v3", "VAL4": "v4"}}}')
    s = Settings()
    assert s.nested.VAL1 == 'v1'
    assert s.nested.SUB_sub.Val2 == 'v2'
    assert s.nested.SUB_sub.SUB_sub_SuB.VaL3 == 'v3'
    assert s.nested.SUB_sub.SUB_sub_SuB.val4 == 'v4'


def test_dotenv_extra_allow(tmp_path):
    p = tmp_path / '.env'
    p.write_text('a=b\nx=y')

    class Settings(BaseSettings):
        a: str

        model_config = SettingsConfigDict(env_file=p, extra='allow')

    s = Settings()
    assert s.a == 'b'
    assert s.x == 'y'


def test_dotenv_extra_forbid(tmp_path):
    p = tmp_path / '.env'
    p.write_text('a=b\nx=y')

    class Settings(BaseSettings):
        a: str

        model_config = SettingsConfigDict(env_file=p, extra='forbid')

    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {'type': 'extra_forbidden', 'loc': ('x',), 'msg': 'Extra inputs are not permitted', 'input': 'y'}
    ]


def test_dotenv_extra_case_insensitive(tmp_path):
    p = tmp_path / '.env'
    p.write_text('a=b')

    class Settings(BaseSettings):
        A: str

        model_config = SettingsConfigDict(env_file=p, extra='forbid')

    s = Settings()
    assert s.A == 'b'


def test_dotenv_extra_sub_model_case_insensitive(tmp_path):
    p = tmp_path / '.env'
    p.write_text('a=b\nSUB_model={"v": "v1"}')

    class SubModel(BaseModel):
        v: str

    class Settings(BaseSettings):
        A: str
        sub_MODEL: SubModel

        model_config = SettingsConfigDict(env_file=p, extra='forbid')

    s = Settings()
    assert s.A == 'b'
    assert s.sub_MODEL.v == 'v1'


def test_nested_bytes_field(env):
    class SubModel(BaseModel):
        v1: str
        v2: bytes

    class Settings(BaseSettings):
        v0: str
        sub_model: SubModel

        model_config = SettingsConfigDict(env_nested_delimiter='__', env_prefix='TEST_')

    env.set('TEST_V0', 'v0')
    env.set('TEST_SUB_MODEL__V1', 'v1')
    env.set('TEST_SUB_MODEL__V2', 'v2')

    s = Settings()

    assert s.v0 == 'v0'
    assert s.sub_model.v1 == 'v1'
    assert s.sub_model.v2 == b'v2'


def test_protected_namespace_defaults():
    # pydantic default
    with pytest.warns(
        UserWarning,
        match=(
            'Field "model_dump_prefixed_field" in Model has conflict with protected namespace "model_dump"|'
            r"Field 'model_dump_prefixed_field' in 'Model' conflicts with protected namespace 'model_dump'\..*"
        ),
    ):

        class Model(BaseSettings):
            model_dump_prefixed_field: str

    # pydantic-settings default
    with pytest.warns(
        UserWarning,
        match=(
            'Field "settings_customise_sources_prefixed_field" in Model1 has conflict with protected namespace "settings_customise_sources"|'
            r"Field 'settings_customise_sources_prefixed_field' in 'Model1' conflicts with protected namespace 'settings_customise_sources'\..*"
        ),
    ):

        class Model1(BaseSettings):
            settings_customise_sources_prefixed_field: str

    with pytest.raises(
        (NameError, ValueError),
        match=(
            r'Field (["\'])settings_customise_sources\1 conflicts with member <bound method '
            r"BaseSettings\.settings_customise_sources of <class 'pydantic_settings\.main\.BaseSettings'>> "
            r'of protected namespace \1settings_customise_sources\1\.'
        ),
    ):

        class Model2(BaseSettings):
            settings_customise_sources: str


def test_case_sensitive_from_args(monkeypatch):
    class Settings(BaseSettings):
        foo: str

    # Need to patch os.environ to get build to work on Windows, where os.environ is case insensitive
    monkeypatch.setattr(os, 'environ', value={'Foo': 'foo'})
    with pytest.raises(ValidationError) as exc_info:
        Settings(_case_sensitive=True)
    assert exc_info.value.errors(include_url=False) == [
        {'type': 'missing', 'loc': ('foo',), 'msg': 'Field required', 'input': {}}
    ]


def test_env_prefix_from_args(env):
    class Settings(BaseSettings):
        apple: str

    env.set('foobar_apple', 'has_prefix')
    s = Settings(_env_prefix='foobar_')
    assert s.apple == 'has_prefix'


def test_env_json_field(env):
    class Settings(BaseSettings):
        x: Json

    env.set('x', '{"foo": "bar"}')

    s = Settings()
    assert s.x == {'foo': 'bar'}

    env.set('x', 'test')
    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'type': 'json_invalid',
            'loc': ('x',),
            'msg': 'Invalid JSON: expected ident at line 1 column 2',
            'input': 'test',
            'ctx': {'error': 'expected ident at line 1 column 2'},
        }
    ]


def test_env_parse_enums(env):
    class NestedEnum(BaseModel):
        fruit: FruitsEnum

    class Settings(BaseSettings, env_nested_delimiter='__'):
        fruit: FruitsEnum
        union_fruit: int | FruitsEnum | None = None
        nested: NestedEnum

    with pytest.raises(ValidationError) as exc_info:
        env.set('FRUIT', 'kiwi')
        env.set('UNION_FRUIT', 'kiwi')
        env.set('NESTED__FRUIT', 'kiwi')
        s = Settings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'type': 'enum',
            'loc': ('fruit',),
            'msg': 'Input should be 0, 1 or 2',
            'input': 'kiwi',
            'ctx': {'expected': '0, 1 or 2'},
        },
        {
            'input': 'kiwi',
            'loc': (
                'union_fruit',
                'int',
            ),
            'msg': 'Input should be a valid integer, unable to parse string as an integer',
            'type': 'int_parsing',
        },
        {
            'ctx': {
                'expected': '0, 1 or 2',
            },
            'input': 'kiwi',
            'loc': (
                'union_fruit',
                'int-enum[FruitsEnum]',
            ),
            'msg': 'Input should be 0, 1 or 2',
            'type': 'enum',
        },
        {
            'ctx': {
                'expected': '0, 1 or 2',
            },
            'input': 'kiwi',
            'loc': (
                'nested',
                'fruit',
            ),
            'msg': 'Input should be 0, 1 or 2',
            'type': 'enum',
        },
    ]

    env.set('FRUIT', str(FruitsEnum.lime.value))
    env.set('UNION_FRUIT', str(FruitsEnum.lime.value))
    env.set('NESTED__FRUIT', str(FruitsEnum.lime.value))
    s = Settings()
    assert s.fruit == FruitsEnum.lime
    assert s.union_fruit == FruitsEnum.lime
    assert s.nested.fruit == FruitsEnum.lime

    env.set('FRUIT', 'kiwi')
    env.set('UNION_FRUIT', 'kiwi')
    env.set('NESTED__FRUIT', 'kiwi')
    s = Settings(_env_parse_enums=True)
    assert s.fruit == FruitsEnum.kiwi
    assert s.union_fruit == FruitsEnum.kiwi
    assert s.nested.fruit == FruitsEnum.kiwi

    env.set('FRUIT', str(FruitsEnum.lime.value))
    env.set('UNION_FRUIT', str(FruitsEnum.lime.value))
    env.set('NESTED__FRUIT', str(FruitsEnum.lime.value))
    s = Settings(_env_parse_enums=True)
    assert s.fruit == FruitsEnum.lime
    assert s.union_fruit == FruitsEnum.lime
    assert s.nested.fruit == FruitsEnum.lime


def test_env_parse_none_str(env):
    env.set('x', 'null')
    env.set('y', 'y_override')

    class Settings(BaseSettings):
        x: str | None = 'x_default'
        y: str | None = 'y_default'

    s = Settings()
    assert s.x == 'null'
    assert s.y == 'y_override'
    s = Settings(_env_parse_none_str='null')
    assert s.x is None
    assert s.y == 'y_override'

    env.set('nested__x', 'None')
    env.set('nested__y', 'y_override')
    env.set('nested__deep__z', 'None')

    class NestedBaseModel(BaseModel):
        x: str | None = 'x_default'
        y: str | None = 'y_default'
        deep: dict | None = {'z': 'z_default'}
        keep: dict | None = {'z': 'None'}

    class NestedSettings(BaseSettings, env_nested_delimiter='__'):
        nested: NestedBaseModel | None = NestedBaseModel()

    s = NestedSettings()
    assert s.nested.x == 'None'
    assert s.nested.y == 'y_override'
    assert s.nested.deep['z'] == 'None'
    assert s.nested.keep['z'] == 'None'
    s = NestedSettings(_env_parse_none_str='None')
    assert s.nested.x is None
    assert s.nested.y == 'y_override'
    assert s.nested.deep['z'] is None
    assert s.nested.keep['z'] == 'None'

    env.set('nested__deep', 'None')

    with pytest.raises(ValidationError):
        s = NestedSettings()
    s = NestedSettings(_env_parse_none_str='None')
    assert s.nested.x is None
    assert s.nested.y == 'y_override'
    assert s.nested.deep['z'] is None
    assert s.nested.keep['z'] == 'None'

    env.pop('nested__deep__z')

    with pytest.raises(ValidationError):
        s = NestedSettings()
    s = NestedSettings(_env_parse_none_str='None')
    assert s.nested.x is None
    assert s.nested.y == 'y_override'
    assert s.nested.deep is None
    assert s.nested.keep['z'] == 'None'


def test_env_json_field_dict(env):
    class Settings(BaseSettings):
        x: Json[dict[str, int]]

    env.set('x', '{"foo": 1}')

    s = Settings()
    assert s.x == {'foo': 1}

    env.set('x', '{"foo": "bar"}')
    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'type': 'int_parsing',
            'loc': ('x', 'foo'),
            'msg': 'Input should be a valid integer, unable to parse string as an integer',
            'input': 'bar',
        }
    ]


def test_custom_env_source_default_values_from_config():
    class CustomEnvSettingsSource(EnvSettingsSource):
        pass

    class Settings(BaseSettings):
        foo: str = 'test'

        model_config = SettingsConfigDict(env_prefix='prefix_', case_sensitive=True)

    s = Settings()
    assert s.model_config['env_prefix'] == 'prefix_'
    assert s.model_config['case_sensitive'] is True

    c = CustomEnvSettingsSource(Settings)
    assert c.env_prefix == 'prefix_'
    assert c.case_sensitive is True


def test_model_config_through_class_kwargs(env):
    class Settings(BaseSettings, env_prefix='foobar_', title='Test Settings Model'):
        apple: str

    assert Settings.model_config['title'] == 'Test Settings Model'  # pydantic config
    assert Settings.model_config['env_prefix'] == 'foobar_'  # pydantic-settings config

    assert Settings.model_json_schema()['title'] == 'Test Settings Model'

    env.set('foobar_apple', 'has_prefix')
    s = Settings()
    assert s.apple == 'has_prefix'


def test_root_model_as_field(env):
    class Foo(BaseModel):
        x: int
        y: dict[str, int]

    FooRoot = RootModel[list[Foo]]

    class Settings(BaseSettings):
        z: FooRoot

    env.set('z', '[{"x": 1, "y": {"foo": 1}}, {"x": 2, "y": {"foo": 2}}]')
    s = Settings()
    assert s.model_dump() == {'z': [{'x': 1, 'y': {'foo': 1}}, {'x': 2, 'y': {'foo': 2}}]}


def test_str_based_root_model(env):
    """Testing to pass string directly to root model."""

    class Foo(RootModel[str]):
        root: str

    class Settings(BaseSettings):
        foo: Foo
        plain: str

    TEST_STR = 'hello world'
    env.set('foo', TEST_STR)
    env.set('plain', TEST_STR)
    s = Settings()
    assert s.model_dump() == {'foo': TEST_STR, 'plain': TEST_STR}


def test_path_based_root_model(env):
    """Testing to pass path directly to root model."""

    class Foo(RootModel[pathlib.PurePosixPath]):
        root: pathlib.PurePosixPath

    class Settings(BaseSettings):
        foo: Foo
        plain: pathlib.PurePosixPath

    TEST_PATH: str = '/hello/world'
    env.set('foo', TEST_PATH)
    env.set('plain', TEST_PATH)
    s = Settings()
    assert s.model_dump() == {
        'foo': pathlib.PurePosixPath(TEST_PATH),
        'plain': pathlib.PurePosixPath(TEST_PATH),
    }


def test_optional_field_from_env(env):
    class Settings(BaseSettings):
        x: str | None = None

    env.set('x', '123')

    s = Settings()
    assert s.x == '123'


def test_dotenv_optional_json_field(tmp_path):
    p = tmp_path / '.env'
    p.write_text("""DATA='{"foo":"bar"}'""")

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_file=p)

        data: Json[dict[str, str]] | None = Field(default=None)

    s = Settings()
    assert s.data == {'foo': 'bar'}


def test_dotenv_with_alias_and_env_prefix(tmp_path):
    p = tmp_path / '.env'
    p.write_text('xxx__foo=1\nxxx__bar=2')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_file=p, env_prefix='xxx__')

        foo: str = ''
        bar_alias: str = Field('', validation_alias='xxx__bar')

    s = Settings()
    assert s.model_dump() == {'foo': '1', 'bar_alias': '2'}

    class Settings1(BaseSettings):
        model_config = SettingsConfigDict(env_file=p, env_prefix='xxx__')

        foo: str = ''
        bar_alias: str = Field('', alias='bar')

    with pytest.raises(ValidationError) as exc_info:
        Settings1()
    assert exc_info.value.errors(include_url=False) == [
        {'type': 'extra_forbidden', 'loc': ('xxx__bar',), 'msg': 'Extra inputs are not permitted', 'input': '2'}
    ]


def test_dotenv_with_alias_and_env_prefix_nested(tmp_path):
    p = tmp_path / '.env'
    p.write_text('xxx__bar=0\nxxx__nested__a=1\nxxx__nested__b=2')

    class NestedSettings(BaseModel):
        a: str = 'a'
        b: str = 'b'

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_prefix='xxx__', env_nested_delimiter='__', env_file=p)

        foo: str = ''
        bar_alias: str = Field('', alias='xxx__bar')
        nested_alias: NestedSettings = Field(default_factory=NestedSettings, alias='xxx__nested')

    s = Settings()
    assert s.model_dump() == {'foo': '', 'bar_alias': '0', 'nested_alias': {'a': '1', 'b': '2'}}


def test_dotenv_with_extra_and_env_prefix(tmp_path):
    p = tmp_path / '.env'
    p.write_text('xxx__foo=1\nxxx__extra_var=extra_value')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(extra='allow', env_file=p, env_prefix='xxx__')

        foo: str = ''

    s = Settings()
    assert s.model_dump() == {'foo': '1', 'extra_var': 'extra_value'}


def test_nested_field_with_alias_init_source():
    class NestedSettings(BaseModel):
        foo: str = Field(alias='fooAlias')

    class Settings(BaseSettings):
        nested_foo: NestedSettings

    s = Settings(nested_foo=NestedSettings(fooAlias='EXAMPLE'))
    assert s.model_dump() == {'nested_foo': {'foo': 'EXAMPLE'}}


def test_nested_models_as_dict_value(env):
    class NestedSettings(BaseModel):
        foo: dict[str, int]

    class Settings(BaseSettings):
        nested: NestedSettings
        sub_dict: dict[str, NestedSettings]

        model_config = SettingsConfigDict(env_nested_delimiter='__')

    env.set('nested__foo', '{"a": 1}')
    env.set('sub_dict__bar__foo', '{"b": 2}')
    s = Settings()
    assert s.model_dump() == {'nested': {'foo': {'a': 1}}, 'sub_dict': {'bar': {'foo': {'b': 2}}}}


def test_env_nested_dict_value(env):
    class Settings(BaseSettings):
        nested: dict[str, dict[str, dict[str, str]]]

        model_config = SettingsConfigDict(env_nested_delimiter='__')

    env.set('nested__foo__a__b', 'bar')
    s = Settings()
    assert s.model_dump() == {'nested': {'foo': {'a': {'b': 'bar'}}}}


def test_nested_models_leaf_vs_deeper_env_dict_assumed(env):
    class NestedSettings(BaseModel):
        foo: str

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__')

        nested: NestedSettings

    env.set('nested__foo', 'string')
    env.set(
        'nested__foo__bar',
        'this should not be evaluated, since foo is a string by annotation and not a dict',
    )
    env.set(
        'nested__foo__bar__baz',
        'one more',
    )
    s = Settings()
    assert s.model_dump() == {'nested': {'foo': 'string'}}


def test_case_insensitive_nested_optional(env):
    class NestedSettings(BaseModel):
        FOO: str
        BaR: int

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__', case_sensitive=False)

        nested: NestedSettings | None

    env.set('nested__FoO', 'string')
    env.set('nested__bar', '123')
    s = Settings()
    assert s.model_dump() == {'nested': {'BaR': 123, 'FOO': 'string'}}


def test_case_insensitive_nested_alias(env):
    """Ensure case-insensitive environment lookup works with nested aliases."""

    class NestedSettings(BaseModel):
        FOO: str = Field(..., alias='Foo')
        BaR: int

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__', case_sensitive=False)

        nEstEd: NestedSettings = Field(..., alias='NesTed')

    env.set('nested__FoO', 'string')
    env.set('nested__bar', '123')
    s = Settings()
    assert s.model_dump() == {'nEstEd': {'BaR': 123, 'FOO': 'string'}}


def test_case_insensitive_nested_list(env):
    class NestedSettings(BaseModel):
        FOO: list[str]

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__', case_sensitive=False)

        nested: NestedSettings | None

    env.set('nested__FOO', '["string1", "string2"]')
    s = Settings()
    assert s.model_dump() == {'nested': {'FOO': ['string1', 'string2']}}


def test_settings_source_current_state(env):
    class SettingsSource(PydanticBaseSettingsSource):
        def get_field_value(self, field: FieldInfo, field_name: str) -> Any:
            pass

        def __call__(self) -> dict[str, Any]:
            current_state = self.current_state
            if current_state.get('one') == '1':
                return {'two': '1'}

            return {}

    class Settings(BaseSettings):
        one: bool = False
        two: bool = False

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (env_settings, SettingsSource(settings_cls))

    env.set('one', '1')
    s = Settings()
    assert s.two is True


def test_settings_source_settings_sources_data(env):
    class SettingsSource(PydanticBaseSettingsSource):
        def get_field_value(self, field: FieldInfo, field_name: str) -> Any:
            pass

        def __call__(self) -> dict[str, Any]:
            settings_sources_data = self.settings_sources_data
            if settings_sources_data == {
                'InitSettingsSource': {'one': True, 'two': True},
                'EnvSettingsSource': {'one': '1'},
                'function_settings_source': {'three': 'false'},
            }:
                return {'four': '1'}

            return {}

    def function_settings_source():
        return {'three': 'false'}

    class Settings(BaseSettings):
        one: bool = False
        two: bool = False
        three: bool = False
        four: bool = False

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (env_settings, init_settings, function_settings_source, SettingsSource(settings_cls))

    env.set('one', '1')
    s = Settings(one=True, two=True)
    assert s.four is True


def test_dotenv_extra_allow_similar_fields(tmp_path):
    p = tmp_path / '.env'
    p.write_text('POSTGRES_USER=postgres\nPOSTGRES_USER_2=postgres2\nPOSTGRES_NAME=name')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_file=p, extra='allow')

        POSTGRES_USER: str

    s = Settings()
    assert s.POSTGRES_USER == 'postgres'
    assert s.model_dump() == {'POSTGRES_USER': 'postgres', 'postgres_name': 'name', 'postgres_user_2': 'postgres2'}


def test_annotation_is_complex_root_model_check():
    """Test for https://github.com/pydantic/pydantic-settings/issues/390"""

    class Settings(BaseSettings):
        foo: list[str] = []

    Settings()


def test_nested_model_field_with_alias(env):
    class NestedSettings(BaseModel):
        foo: list[str] = Field(alias='fooalias')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__')

        nested: NestedSettings

    env.set('nested__fooalias', '["one", "two"]')

    s = Settings()
    assert s.model_dump() == {'nested': {'foo': ['one', 'two']}}


def test_nested_model_field_with_alias_case_sensitive(monkeypatch):
    class NestedSettings(BaseModel):
        foo: list[str] = Field(alias='fooAlias')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__', case_sensitive=True)

        nested: NestedSettings

    # Need to patch os.environ to get build to work on Windows, where os.environ is case insensitive
    monkeypatch.setattr(os, 'environ', value={'nested__fooalias': '["one", "two"]'})
    with pytest.raises(ValidationError) as exc_info:
        Settings()
    assert exc_info.value.errors(include_url=False) == [
        {
            'type': 'missing',
            'loc': ('nested', 'fooAlias'),
            'msg': 'Field required',
            'input': {'fooalias': '["one", "two"]'},
        }
    ]

    monkeypatch.setattr(os, 'environ', value={'nested__fooAlias': '["one", "two"]'})
    s = Settings()
    assert s.model_dump() == {'nested': {'foo': ['one', 'two']}}


def test_nested_model_field_with_alias_choices(env):
    class NestedSettings(BaseModel):
        foo: list[str] = Field(alias=AliasChoices('fooalias', 'foo-alias'))

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(env_nested_delimiter='__')

        nested: NestedSettings

    env.set('nested__fooalias', '["one", "two"]')

    s = Settings()
    assert s.model_dump() == {'nested': {'foo': ['one', 'two']}}


def test_dotenv_optional_nested(tmp_path):
    p = tmp_path / '.env'
    p.write_text('not_nested=works\nNESTED__A=fails\nNESTED__b=2')

    class NestedSettings(BaseModel):
        A: str
        b: int

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            env_file=p,
            env_nested_delimiter='__',
            extra='forbid',
        )

        not_nested: str
        NESTED: NestedSettings | None

    s = Settings()
    assert s.model_dump() == {'not_nested': 'works', 'NESTED': {'A': 'fails', 'b': 2}}


def test_dotenv_env_prefix_env_without_prefix(tmp_path):
    p = tmp_path / '.env'
    p.write_text('test_foo=test-foo\nfoo=foo')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            env_file=p,
            env_prefix='TEST_',
            extra='ignore',
        )

        foo: str

    s = Settings()
    assert s.model_dump() == {'foo': 'test-foo'}


def test_dotenv_env_prefix_env_without_prefix_ignored(tmp_path):
    p = tmp_path / '.env'
    p.write_text('foo=foo')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            env_file=p,
            env_prefix='TEST_',
            extra='ignore',
        )

        foo: str = ''

    s = Settings()
    assert s.model_dump() == {'foo': ''}


def test_nested_model_dotenv_env_prefix_env_without_prefix_ignored(tmp_path):
    p = tmp_path / '.env'
    p.write_text('foo__val=1')

    class Foo(BaseModel):
        val: int = 0

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            env_nested_delimiter='__',
            env_file=p,
            env_prefix='TEST_',
            extra='ignore',
        )

        foo: Foo = Foo()

    s = Settings()
    assert s.model_dump() == {'foo': {'val': 0}}


def test_dotenv_env_prefix_env_with_alias_without_prefix(tmp_path):
    p = tmp_path / '.env'
    p.write_text('FooAlias=foo')

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            env_file=p,
            env_prefix='TEST_',
            extra='ignore',
        )

        foo: str = Field('xxx', alias='FooAlias')

    s = Settings()
    assert s.model_dump() == {'foo': 'foo'}


def test_parsing_secret_field(env):
    class Settings(BaseSettings):
        foo: Secret[int]
        bar: Secret[PostgresDsn]

    env.set('foo', '123')
    env.set('bar', 'postgres://user:password@localhost/dbname')

    s = Settings()
    assert s.foo.get_secret_value() == 123
    assert s.bar.get_secret_value() == PostgresDsn('postgres://user:password@localhost/dbname')


def test_field_annotated_no_decode(env):
    class Settings(BaseSettings):
        a: list[str]  # this field will be decoded because of default `enable_decoding=True`
        b: Annotated[list[str], NoDecode]

        # decode the value here. the field value won't be decoded because of NoDecode
        @field_validator('b', mode='before')
        @classmethod
        def decode_b(cls, v: str) -> list[str]:
            return json.loads(v)

    env.set('a', '["one", "two"]')
    env.set('b', '["1", "2"]')

    s = Settings()
    assert s.model_dump() == {'a': ['one', 'two'], 'b': ['1', '2']}


def test_field_annotated_no_decode_and_disable_decoding(env):
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(enable_decoding=False)

        a: Annotated[list[str], NoDecode]

        # decode the value here. the field value won't be decoded because of NoDecode
        @field_validator('a', mode='before')
        @classmethod
        def decode_b(cls, v: str) -> list[str]:
            return json.loads(v)

    env.set('a', '["one", "two"]')

    s = Settings()
    assert s.model_dump() == {'a': ['one', 'two']}


def test_field_annotated_disable_decoding(env):
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(enable_decoding=False)

        a: list[str]

        # decode the value here. the field value won't be decoded because of `enable_decoding=False`
        @field_validator('a', mode='before')
        @classmethod
        def decode_b(cls, v: str) -> list[str]:
            return json.loads(v)

    env.set('a', '["one", "two"]')

    s = Settings()
    assert s.model_dump() == {'a': ['one', 'two']}


def test_field_annotated_force_decode_disable_decoding(env):
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(enable_decoding=False)

        a: Annotated[list[str], ForceDecode]

    env.set('a', '["one", "two"]')

    s = Settings()
    assert s.model_dump() == {'a': ['one', 'two']}


def test_warns_if_config_keys_are_set_but_source_is_missing():
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            json_file='config.json',
            pyproject_toml_depth=2,
            toml_file='config.toml',
            yaml_file='config.yaml',
            yaml_config_section='myapp',
        )

    with pytest.warns() as record:
        Settings()

    assert len(record) == 5

    key_class_pairs = [
        ('json_file', 'JsonConfigSettingsSource'),
        ('pyproject_toml_depth', 'PyprojectTomlConfigSettingsSource'),
        ('toml_file', 'TomlConfigSettingsSource'),
        ('yaml_file', 'YamlConfigSettingsSource'),
        ('yaml_config_section', 'YamlConfigSettingsSource'),
    ]

    for warning, key_class_pair in zip(record, key_class_pairs):
        assert warning.category is UserWarning
        expected_message = (
            f'Config key `{key_class_pair[0]}` is set in model_config but will be ignored because no '
            f'{key_class_pair[1]} source is configured. To use this config key, add a {key_class_pair[1]} '
            f'source to the settings sources via the settings_customise_sources hook.'
        )
        assert warning.message.args[0] == expected_message


def test_env_strict_coercion(env):
    class SubModel(BaseModel):
        my_str: str
        my_int: int

    class Settings(BaseSettings, env_nested_delimiter='__'):
        my_str: str
        my_int: int
        sub_model: SubModel

    env.set('MY_STR', '0')
    env.set('MY_INT', '0')
    env.set('SUB_MODEL__MY_STR', '1')
    env.set('SUB_MODEL__MY_INT', '1')
    Settings().model_dump() == {
        'my_str': '0',
        'my_int': 0,
        'sub_model': {
            'my_str': '1',
            'my_int': 1,
        },
    }

    class StrictSettings(BaseSettings, env_nested_delimiter='__', strict=True):
        my_str: str
        my_int: int
        sub_model: SubModel

    StrictSettings().model_dump() == {
        'my_str': '0',
        'my_int': 0,
        'sub_model': {
            'my_str': '1',
            'my_int': 1,
        },
    }
