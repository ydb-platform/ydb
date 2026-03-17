import argparse
import asyncio
import re
import sys
import time
import typing
from enum import IntEnum
from pathlib import Path, PureWindowsPath
from typing import Annotated, Any, Dict, Generic, List, Literal, Tuple, TypeVar, Union  # noqa: UP035

import pytest
import typing_extensions
from pydantic import (
    AliasChoices,
    AliasGenerator,
    AliasPath,
    BaseModel,
    ConfigDict,
    DirectoryPath,
    Discriminator,
    Field,
    RootModel,
    Tag,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic import (
    dataclasses as pydantic_dataclasses,
)
from pydantic._internal._repr import Representation

from pydantic_settings import (
    BaseSettings,
    CliApp,
    ForceDecode,
    NoDecode,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    SettingsError,
)
from pydantic_settings.sources import (
    CLI_SUPPRESS,
    CliExplicitFlag,
    CliImplicitFlag,
    CliMutuallyExclusiveGroup,
    CliPositionalArg,
    CliSettingsSource,
    CliSubCommand,
    CliSuppress,
    CliUnknownArgs,
    get_subcommand,
)

ARGPARSE_OPTIONS_TEXT = 'options' if sys.version_info >= (3, 10) else 'optional arguments'
PYTHON_3_14 = sys.version_info >= (3, 14)
IS_WINDOWS = sys.platform.startswith('win')


def sanitize_cli_output(output: str) -> str:
    output = re.sub(r'\bpython3? ', '', output)
    output = output.replace('python.exe -m pytest', 'example.py')  # For Windows compatibility
    return output


@pytest.fixture(autouse=True)
def cli_test_env_autouse(cli_test_env):
    pass


def foobar(a, b, c=4):
    pass


class FruitsEnum(IntEnum):
    pear = 0
    kiwi = 1
    lime = 2


T = TypeVar('T')


class LoggedVar(Generic[T]):
    def get(self) -> T: ...


class SimpleSettings(BaseSettings):
    apple: str


class SettingWithIgnoreEmpty(BaseSettings):
    apple: str = 'default'

    model_config = SettingsConfigDict(env_ignore_empty=True)


class CliDummyArgGroup(BaseModel, arbitrary_types_allowed=True):
    group: argparse._ArgumentGroup

    def add_argument(self, *args: Any, **kwargs: Any) -> None:
        self.group.add_argument(*args, **kwargs)


class CliDummySubParsers(BaseModel, arbitrary_types_allowed=True):
    sub_parser: argparse._SubParsersAction

    def add_parser(self, *args: Any, **kwargs: Any) -> 'CliDummyParser':
        return CliDummyParser(parser=self.sub_parser.add_parser(*args, **kwargs))


class CliDummyParser(BaseModel, arbitrary_types_allowed=True):
    parser: argparse.ArgumentParser = Field(default_factory=lambda: argparse.ArgumentParser())

    def add_argument(self, *args: Any, **kwargs: Any) -> None:
        self.parser.add_argument(*args, **kwargs)

    def add_argument_group(self, *args: Any, **kwargs: Any) -> CliDummyArgGroup:
        return CliDummyArgGroup(group=self.parser.add_argument_group(*args, **kwargs))

    def add_subparsers(self, *args: Any, **kwargs: Any) -> CliDummySubParsers:
        return CliDummySubParsers(sub_parser=self.parser.add_subparsers(*args, **kwargs))

    def parse_args(self, *args: Any, **kwargs: Any) -> argparse.Namespace:
        return self.parser.parse_args(*args, **kwargs)


def test_cli_validation_alias_with_cli_prefix():
    class Settings(BaseSettings, cli_exit_on_error=False):
        foobar: str = Field(validation_alias='foo')

        model_config = SettingsConfigDict(cli_prefix='p')

    with pytest.raises(SettingsError, match='error parsing CLI: unrecognized arguments: --foo bar'):
        CliApp.run(Settings, cli_args=['--foo', 'bar'])

    assert CliApp.run(Settings, cli_args=['--p.foo', 'bar']).foobar == 'bar'


@pytest.mark.parametrize(
    'alias_generator',
    [
        AliasGenerator(validation_alias=lambda s: AliasChoices(s, s.replace('_', '-'))),
        AliasGenerator(validation_alias=lambda s: AliasChoices(s.replace('_', '-'), s)),
    ],
)
def test_cli_alias_resolution_consistency_with_env(env, alias_generator):
    class SubModel(BaseModel):
        v1: str = 'model default'

    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            env_nested_delimiter='__',
            nested_model_default_partial_update=True,
            alias_generator=alias_generator,
        )

        sub_model: SubModel = SubModel(v1='top default')

    assert CliApp.run(Settings, cli_args=[]).model_dump() == {'sub_model': {'v1': 'top default'}}

    env.set('SUB_MODEL__V1', 'env default')
    assert CliApp.run(Settings, cli_args=[]).model_dump() == {'sub_model': {'v1': 'env default'}}

    assert CliApp.run(Settings, cli_args=['--sub-model.v1=cli default']).model_dump() == {
        'sub_model': {'v1': 'cli default'}
    }


def test_cli_nested_arg():
    class SubSubValue(BaseModel):
        v6: str

    class SubValue(BaseModel):
        v4: str
        v5: int
        sub_sub: SubSubValue

    class TopValue(BaseModel):
        v1: str
        v2: str
        v3: str
        sub: SubValue

    class Cfg(BaseSettings):
        v0: str
        v0_union: SubValue | int
        top: TopValue

    args: list[str] = []
    args += ['--top', '{"v1": "json-1", "v2": "json-2", "sub": {"v5": "xx"}}']
    args += ['--top.sub.v5', '5']
    args += ['--v0', '0']
    args += ['--top.v2', '2']
    args += ['--top.v3', '3']
    args += ['--v0_union', '0']
    args += ['--top.sub.sub_sub.v6', '6']
    args += ['--top.sub.v4', '4']
    cfg = CliApp.run(Cfg, cli_args=args)
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


def test_cli_source_prioritization(env):
    class CfgDefault(BaseSettings):
        foo: str

    class CfgPrioritized(BaseSettings):
        foo: str

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return env_settings, CliSettingsSource(settings_cls, cli_parse_args=['--foo', 'FOO FROM CLI'])

    env.set('FOO', 'FOO FROM ENV')

    cfg = CliApp.run(CfgDefault, cli_args=['--foo', 'FOO FROM CLI'])
    assert cfg.model_dump() == {'foo': 'FOO FROM CLI'}

    cfg = CfgPrioritized()
    assert cfg.model_dump() == {'foo': 'FOO FROM ENV'}


def test_cli_alias_subcommand_and_positional_args(capsys, monkeypatch):
    class SubCmd(BaseModel):
        pos_arg: CliPositionalArg[str] = Field(validation_alias='pos-arg')

    class Cfg(BaseSettings):
        sub_cmd: CliSubCommand[SubCmd] = Field(validation_alias='sub-cmd')

    cfg = Cfg(**{'sub-cmd': {'pos-arg': 'howdy'}})
    assert cfg.model_dump() == {'sub_cmd': {'pos_arg': 'howdy'}}

    cfg = CliApp.run(Cfg, cli_args=['sub-cmd', 'howdy'])
    assert cfg.model_dump() == {'sub_cmd': {'pos_arg': 'howdy'}}

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            CliApp.run(Cfg)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] {{sub-cmd}} ...

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help  show this help message and exit

subcommands:
  {{sub-cmd}}
    sub-cmd
"""
        )
        m.setattr(sys, 'argv', ['example.py', 'sub-cmd', '--help'])

        with pytest.raises(SystemExit):
            CliApp.run(Cfg)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py sub-cmd [-h] POS-ARG

positional arguments:
  POS-ARG

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help  show this help message and exit
"""
        )


@pytest.mark.parametrize('avoid_json', [True, False])
def test_cli_alias_arg(capsys, monkeypatch, avoid_json):
    class Cfg(BaseSettings, cli_avoid_json=avoid_json):
        alias_choice_w_path: str = Field(validation_alias=AliasChoices('a', AliasPath('path0', 1)))
        alias_choice_w_only_path: str = Field(validation_alias=AliasChoices(AliasPath('path1', 1)))
        alias_choice_no_path: str = Field(validation_alias=AliasChoices('b', 'c'))
        alias_path: str = Field(validation_alias=AliasPath('path2', 'deep', 1))
        alias_extra_deep: str = Field(validation_alias=AliasPath('path3', 'deep', 'extra', 'deep', 1))
        alias_str: str = Field(validation_alias='str')

    cfg = CliApp.run(
        Cfg,
        cli_args=[
            '-a',
            'a',
            '-b',
            'b',
            '--str',
            'str',
            '--path0',
            'a0,b0,c0',
            '--path1',
            'a1,b1,c1',
            '--path2',
            '{"deep": ["a2","b2","c2"]}',
            '--path3',
            '{"deep": {"extra": {"deep": ["a3","b3","c3"]}}}',
        ],
    )
    assert cfg.model_dump() == {
        'alias_choice_w_path': 'a',
        'alias_choice_w_only_path': 'b1',
        'alias_choice_no_path': 'b',
        'alias_path': 'b2',
        'alias_extra_deep': 'b3',
        'alias_str': 'str',
    }

    serialized_cli_args = CliApp.serialize(cfg)
    assert serialized_cli_args == [
        '-a',
        'a',
        '--path1',
        '["", "b1"]',
        '-b',
        'b',
        '--path2',
        '{"deep": ["", "b2"]}',
        '--path3',
        '{"deep": {"extra": {"deep": ["", "b3"]}}}',
        '--str',
        'str',
    ]
    assert CliApp.run(Cfg, cli_args=serialized_cli_args).model_dump() == cfg.model_dump()


@pytest.mark.parametrize('avoid_json', [True, False])
def test_cli_alias_nested_arg(capsys, monkeypatch, avoid_json):
    class Nested(BaseModel):
        alias_choice_w_path: str = Field(validation_alias=AliasChoices('a', AliasPath('path0', 1)))
        alias_choice_w_only_path: str = Field(validation_alias=AliasChoices(AliasPath('path1', 1)))
        alias_choice_no_path: str = Field(validation_alias=AliasChoices('b', 'c'))
        alias_path: str = Field(validation_alias=AliasPath('path2', 'deep', 1))
        alias_extra_deep: str = Field(validation_alias=AliasPath('path3', 'deep', 'extra', 'deep', 1))
        alias_str: str = Field(validation_alias='str')

    class Cfg(BaseSettings, cli_avoid_json=avoid_json):
        nest: Nested

    cfg = CliApp.run(
        Cfg,
        cli_args=[
            '--nest.a',
            'a',
            '--nest.b',
            'b',
            '--nest.str',
            'str',
            '--nest.path0',
            '["a0","b0","c0"]',
            '--nest.path1',
            '["a1","b1","c1"]',
            '--nest.path2',
            '{"deep": ["a2","b2","c2"]}',
            '--nest.path3',
            '{"deep": {"extra": {"deep": ["a3","b3","c3"]}}}',
        ],
    )
    assert cfg.model_dump() == {
        'nest': {
            'alias_choice_w_path': 'a',
            'alias_choice_w_only_path': 'b1',
            'alias_choice_no_path': 'b',
            'alias_path': 'b2',
            'alias_extra_deep': 'b3',
            'alias_str': 'str',
        }
    }

    serialized_cli_args = CliApp.serialize(cfg)
    assert serialized_cli_args == [
        '--nest.a',
        'a',
        '--nest.path1',
        '["", "b1"]',
        '--nest.b',
        'b',
        '--nest.path2',
        '{"deep": ["", "b2"]}',
        '--nest.path3',
        '{"deep": {"extra": {"deep": ["", "b3"]}}}',
        '--nest.str',
        'str',
    ]
    assert CliApp.run(Cfg, cli_args=serialized_cli_args).model_dump() == cfg.model_dump()


def test_cli_alias_exceptions(capsys, monkeypatch):
    with pytest.raises(SettingsError, match='subcommand argument BadCliSubCommand.foo has multiple aliases'):

        class SubCmd(BaseModel):
            v0: int

        class BadCliSubCommand(BaseSettings):
            foo: CliSubCommand[SubCmd] = Field(validation_alias=AliasChoices('bar', 'boo'))

        CliApp.run(BadCliSubCommand)

    with pytest.raises(SettingsError, match='positional argument BadCliPositionalArg.foo has multiple alias'):

        class BadCliPositionalArg(BaseSettings):
            foo: CliPositionalArg[int] = Field(validation_alias=AliasChoices('bar', 'boo'))

        CliApp.run(BadCliPositionalArg)


def test_cli_case_insensitive_arg():
    class Cfg(BaseSettings, cli_exit_on_error=False):
        foo: str = Field(validation_alias=AliasChoices('F', 'Foo'))
        bar: str = Field(validation_alias=AliasChoices('B', 'Bar'))

    cfg = CliApp.run(
        Cfg,
        cli_args=[
            '--FOO=--VAL',
            '--BAR',
            '"--VAL"',
        ],
    )
    assert cfg.model_dump() == {'foo': '--VAL', 'bar': '"--VAL"'}

    cfg = CliApp.run(
        Cfg,
        cli_args=[
            '-f=-V',
            '-b',
            '"-V"',
        ],
    )
    assert cfg.model_dump() == {'foo': '-V', 'bar': '"-V"'}

    cfg = Cfg(_cli_parse_args=['--Foo=--VAL', '--Bar', '"--VAL"'], _case_sensitive=True)
    assert cfg.model_dump() == {'foo': '--VAL', 'bar': '"--VAL"'}

    cfg = Cfg(_cli_parse_args=['-F=-V', '-B', '"-V"'], _case_sensitive=True)
    assert cfg.model_dump() == {'foo': '-V', 'bar': '"-V"'}

    with pytest.raises(SettingsError, match='error parsing CLI: unrecognized arguments: --FOO=--VAL --BAR "--VAL"'):
        Cfg(_cli_parse_args=['--FOO=--VAL', '--BAR', '"--VAL"'], _case_sensitive=True)

    with pytest.raises(SettingsError, match='error parsing CLI: unrecognized arguments: -f=-V -b "-V"'):
        Cfg(_cli_parse_args=['-f=-V', '-b', '"-V"'], _case_sensitive=True)

    with pytest.raises(SettingsError, match='Case-insensitive matching is only supported on the internal root parser'):
        CliSettingsSource(Cfg, root_parser=CliDummyParser(), case_sensitive=False)


def test_cli_help_differentiation(capsys, monkeypatch):
    class Cfg(BaseSettings):
        foo: str
        bar: int = 123
        boo: int = Field(default_factory=lambda: 456)

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            CliApp.run(Cfg)

        assert (
            re.sub(r'0x\w+', '0xffffffff', sanitize_cli_output(capsys.readouterr().out), flags=re.MULTILINE)
            == f"""usage: example.py [-h] [--foo str] [--bar int] [--boo int]

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help  show this help message and exit
  --foo str   (required)
  --bar int   (default: 123)
  --boo int   (default factory: <lambda>)
"""
        )


def test_cli_help_string_format(capsys, monkeypatch):
    class Cfg(BaseSettings, cli_parse_args=True):
        date_str: str = '%Y-%m-%d'

    class MultilineDoc(BaseSettings, cli_parse_args=True):
        """
        My
        Multiline
        Doc
        """

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            Cfg()

        assert (
            re.sub(r'0x\w+', '0xffffffff', sanitize_cli_output(capsys.readouterr().out), flags=re.MULTILINE)
            == f"""usage: example.py [-h] [--date_str str]

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help      show this help message and exit
  --date_str str  (default: %Y-%m-%d)
"""
        )

        with pytest.raises(SystemExit):
            MultilineDoc()
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h]

My
Multiline
Doc

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help  show this help message and exit
"""
        )

        with pytest.raises(SystemExit):
            cli_settings_source = CliSettingsSource(MultilineDoc, formatter_class=argparse.HelpFormatter)
            MultilineDoc(_cli_settings_source=cli_settings_source(args=True))
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h]

My Multiline Doc

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help  show this help message and exit
"""
        )


def test_cli_help_union_of_models(capsys, monkeypatch):
    class Cat(BaseModel):
        meow: str = 'meow'

    class Dog(BaseModel):
        bark: str = 'bark'

    class Bird(BaseModel):
        caww: str = 'caww'
        tweet: str

    class Tiger(Cat):
        roar: str = 'roar'

    class Car(BaseSettings, cli_parse_args=True):
        driver: Cat | Dog | Bird = Tiger(meow='purr')

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        if PYTHON_3_14:
            if IS_WINDOWS:
                usage = '                            [--driver.bark str] [--driver.caww str]\n                            [--driver.tweet str]'
            else:
                usage = '                         [--driver.bark str] [--driver.caww str]\n                         [--driver.tweet str]'
        else:
            usage = '                  [--driver.bark str] [--driver.caww str] [--driver.tweet str]'
        with pytest.raises(SystemExit):
            Car()
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--driver [JSON]] [--driver.meow str]
{usage}

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help          show this help message and exit

driver options:
  --driver [JSON]     set driver from JSON string (default: {{}})
  --driver.meow str   (default: purr)
  --driver.bark str   (default: bark)
  --driver.caww str   (default: caww)
  --driver.tweet str  (ifdef: required)
"""
        )


def test_cli_help_default_or_none_model(capsys, monkeypatch):
    class DeeperSubModel(BaseModel):
        flag: bool

    class DeepSubModel(BaseModel):
        flag: bool
        deeper: DeeperSubModel | None = None

    class SubModel(BaseModel):
        flag: bool
        deep: DeepSubModel = DeepSubModel(flag=True)

    class Settings(BaseSettings, cli_parse_args=True):
        flag: bool = True
        sub_model: SubModel = SubModel(flag=False)
        opt_model: DeepSubModel | None = Field(None, description='Group Doc')
        fact_model: SubModel = Field(default_factory=lambda: SubModel(flag=True))

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        if PYTHON_3_14:
            if IS_WINDOWS:
                text = """                            [--sub_model.flag bool] [--sub_model.deep [JSON]]
                            [--sub_model.deep.flag bool]
                            [--sub_model.deep.deeper [{JSON,null}]]
                            [--sub_model.deep.deeper.flag bool]
                            [--opt_model [{JSON,null}]]
                            [--opt_model.flag bool]
                            [--opt_model.deeper [{JSON,null}]]
                            [--opt_model.deeper.flag bool]
                            [--fact_model [JSON]] [--fact_model.flag bool]
                            [--fact_model.deep [JSON]]
                            [--fact_model.deep.flag bool]
                            [--fact_model.deep.deeper [{JSON,null}]]
                            [--fact_model.deep.deeper.flag bool]
"""
            else:
                text = """                         [--sub_model.flag bool] [--sub_model.deep [JSON]]
                         [--sub_model.deep.flag bool]
                         [--sub_model.deep.deeper [{JSON,null}]]
                         [--sub_model.deep.deeper.flag bool]
                         [--opt_model [{JSON,null}]] [--opt_model.flag bool]
                         [--opt_model.deeper [{JSON,null}]]
                         [--opt_model.deeper.flag bool] [--fact_model [JSON]]
                         [--fact_model.flag bool] [--fact_model.deep [JSON]]
                         [--fact_model.deep.flag bool]
                         [--fact_model.deep.deeper [{JSON,null}]]
                         [--fact_model.deep.deeper.flag bool]
"""
        else:
            text = """                  [--sub_model.flag bool] [--sub_model.deep [JSON]]
                  [--sub_model.deep.flag bool]
                  [--sub_model.deep.deeper [{JSON,null}]]
                  [--sub_model.deep.deeper.flag bool]
                  [--opt_model [{JSON,null}]] [--opt_model.flag bool]
                  [--opt_model.deeper [{JSON,null}]]
                  [--opt_model.deeper.flag bool] [--fact_model [JSON]]
                  [--fact_model.flag bool] [--fact_model.deep [JSON]]
                  [--fact_model.deep.flag bool]
                  [--fact_model.deep.deeper [{JSON,null}]]
                  [--fact_model.deep.deeper.flag bool]
"""
        with pytest.raises(SystemExit):
            Settings()
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--flag bool] [--sub_model [JSON]]
{text}
{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit
  --flag bool           (default: True)

sub_model options:
  --sub_model [JSON]    set sub_model from JSON string (default: {{}})
  --sub_model.flag bool
                        (default: False)

sub_model.deep options:
  --sub_model.deep [JSON]
                        set sub_model.deep from JSON string (default: {{}})
  --sub_model.deep.flag bool
                        (default: True)

sub_model.deep.deeper options:
  default: null (undefined)

  --sub_model.deep.deeper [{{JSON,null}}]
                        set sub_model.deep.deeper from JSON string (default:
                        {{}})
  --sub_model.deep.deeper.flag bool
                        (ifdef: required)

opt_model options:
  default: null (undefined)
  Group Doc

  --opt_model [{{JSON,null}}]
                        set opt_model from JSON string (default: {{}})
  --opt_model.flag bool
                        (ifdef: required)

opt_model.deeper options:
  default: null (undefined)

  --opt_model.deeper [{{JSON,null}}]
                        set opt_model.deeper from JSON string (default: {{}})
  --opt_model.deeper.flag bool
                        (ifdef: required)

fact_model options:
  --fact_model [JSON]   set fact_model from JSON string (default: {{}})
  --fact_model.flag bool
                        (default factory: <lambda>)

fact_model.deep options:
  --fact_model.deep [JSON]
                        set fact_model.deep from JSON string (default: {{}})
  --fact_model.deep.flag bool
                        (default factory: <lambda>)

fact_model.deep.deeper options:
  --fact_model.deep.deeper [{{JSON,null}}]
                        set fact_model.deep.deeper from JSON string (default:
                        {{}})
  --fact_model.deep.deeper.flag bool
                        (default factory: <lambda>)
"""
        )


def test_cli_nested_dataclass_arg():
    @pydantic_dataclasses.dataclass
    class MyDataclass:
        foo: int
        bar: str

    class Settings(BaseSettings):
        n: MyDataclass

    s = CliApp.run(Settings, cli_args=['--n.foo', '123', '--n.bar', 'bar value'])
    assert isinstance(s.n, MyDataclass)
    assert s.n.foo == 123
    assert s.n.bar == 'bar value'


def no_add_cli_arg_spaces(arg_str: str, has_quote_comma: bool = False) -> str:
    return arg_str


def add_cli_arg_spaces(arg_str: str, has_quote_comma: bool = False) -> str:
    arg_str = arg_str.replace('[', ' [ ')
    arg_str = arg_str.replace(']', ' ] ')
    arg_str = arg_str.replace('{', ' { ')
    arg_str = arg_str.replace('}', ' } ')
    arg_str = arg_str.replace(':', ' : ')
    if not has_quote_comma:
        arg_str = arg_str.replace(',', ' , ')
    else:
        arg_str = arg_str.replace('",', '" , ')
    return f' {arg_str} '


@pytest.mark.parametrize('arg_spaces', [no_add_cli_arg_spaces, add_cli_arg_spaces])
@pytest.mark.parametrize('prefix', ['', 'child.'])
def test_cli_list_arg(prefix, arg_spaces):
    class Obj(BaseModel):
        val: int

    class Child(BaseModel):
        num_list: list[int] | None = None
        obj_list: list[Obj] | None = None
        str_list: list[str] | None = None
        union_list: list[Obj | int] | None = None

    class Cfg(BaseSettings):
        num_list: list[int] | None = None
        obj_list: list[Obj] | None = None
        union_list: list[Obj | int] | None = None
        str_list: list[str] | None = None
        child: Child | None = None

    def check_answer(cfg, prefix, expected):
        if prefix:
            assert cfg.model_dump() == {
                'num_list': None,
                'obj_list': None,
                'union_list': None,
                'str_list': None,
                'child': expected,
            }
        else:
            expected['child'] = None
            assert cfg.model_dump() == expected

    args: list[str] = []
    args = [f'--{prefix}num_list', arg_spaces('[1,2]')]
    args += [f'--{prefix}num_list', arg_spaces('3,4')]
    args += [f'--{prefix}num_list', '5', f'--{prefix}num_list', '6']
    cfg = CliApp.run(Cfg, cli_args=args)
    expected = {
        'num_list': [1, 2, 3, 4, 5, 6],
        'obj_list': None,
        'union_list': None,
        'str_list': None,
    }
    check_answer(cfg, prefix, expected)

    args = [f'--{prefix}obj_list', arg_spaces('[{"val":1},{"val":2}]')]
    args += [f'--{prefix}obj_list', arg_spaces('{"val":3},{"val":4}')]
    args += [f'--{prefix}obj_list', arg_spaces('{"val":5}'), f'--{prefix}obj_list', arg_spaces('{"val":6}')]
    cfg = CliApp.run(Cfg, cli_args=args)
    expected = {
        'num_list': None,
        'obj_list': [{'val': 1}, {'val': 2}, {'val': 3}, {'val': 4}, {'val': 5}, {'val': 6}],
        'union_list': None,
        'str_list': None,
    }
    check_answer(cfg, prefix, expected)

    args = [f'--{prefix}union_list', arg_spaces('[{"val":1},2]'), f'--{prefix}union_list', arg_spaces('[3,{"val":4}]')]
    args += [f'--{prefix}union_list', arg_spaces('{"val":5},6'), f'--{prefix}union_list', arg_spaces('7,{"val":8}')]
    args += [f'--{prefix}union_list', arg_spaces('{"val":9}'), f'--{prefix}union_list', '10']
    cfg = CliApp.run(Cfg, cli_args=args)
    expected = {
        'num_list': None,
        'obj_list': None,
        'union_list': [{'val': 1}, 2, 3, {'val': 4}, {'val': 5}, 6, 7, {'val': 8}, {'val': 9}, 10],
        'str_list': None,
    }
    check_answer(cfg, prefix, expected)

    args = [f'--{prefix}str_list', arg_spaces('["0,0","1,1"]', has_quote_comma=True)]
    args += [f'--{prefix}str_list', arg_spaces('"2,2","3,3"', has_quote_comma=True)]
    args += [
        f'--{prefix}str_list',
        arg_spaces('"4,4"', has_quote_comma=True),
        f'--{prefix}str_list',
        arg_spaces('"5,5"', has_quote_comma=True),
    ]
    cfg = CliApp.run(Cfg, cli_args=args)
    expected = {
        'num_list': None,
        'obj_list': None,
        'union_list': None,
        'str_list': ['0,0', '1,1', '2,2', '3,3', '4,4', '5,5'],
    }
    check_answer(cfg, prefix, expected)


@pytest.mark.parametrize('arg_spaces', [no_add_cli_arg_spaces, add_cli_arg_spaces])
def test_cli_list_json_value_parsing(arg_spaces):
    class Cfg(BaseSettings):
        json_list: list[str | bool | None]

    assert CliApp.run(
        Cfg,
        cli_args=[
            '--json_list',
            arg_spaces('true,"true"'),
            '--json_list',
            arg_spaces('false,"false"'),
            '--json_list',
            arg_spaces('null,"null"'),
            '--json_list',
            arg_spaces('hi,"bye"'),
        ],
    ).model_dump() == {'json_list': [True, 'true', False, 'false', None, 'null', 'hi', 'bye']}

    assert CliApp.run(Cfg, cli_args=['--json_list', '"","","",""']).model_dump() == {'json_list': ['', '', '', '']}
    assert CliApp.run(Cfg, cli_args=['--json_list', ',,,']).model_dump() == {'json_list': ['', '', '', '']}


@pytest.mark.parametrize('arg_spaces', [no_add_cli_arg_spaces, add_cli_arg_spaces])
@pytest.mark.parametrize('prefix', ['', 'child.'])
def test_cli_dict_arg(prefix, arg_spaces):
    class Child(BaseModel):
        check_dict: dict[str, str]

    class Cfg(BaseSettings):
        check_dict: dict[str, str] | None = None
        child: Child | None = None

    args: list[str] = []
    args = [f'--{prefix}check_dict', arg_spaces('{"k1":"a","k2":"b"}')]
    args += [f'--{prefix}check_dict', arg_spaces('{"k3":"c"},{"k4":"d"}')]
    args += [f'--{prefix}check_dict', arg_spaces('{"k5":"e"}'), f'--{prefix}check_dict', arg_spaces('{"k6":"f"}')]
    args += [f'--{prefix}check_dict', arg_spaces('[k7=g,k8=h]')]
    args += [f'--{prefix}check_dict', arg_spaces('k9=i,k10=j')]
    args += [f'--{prefix}check_dict', arg_spaces('k11=k'), f'--{prefix}check_dict', arg_spaces('k12=l')]
    args += [
        f'--{prefix}check_dict',
        arg_spaces('[{"k13":"m"},k14=n]'),
        f'--{prefix}check_dict',
        arg_spaces('[k15=o,{"k16":"p"}]'),
    ]
    args += [
        f'--{prefix}check_dict',
        arg_spaces('{"k17":"q"},k18=r'),
        f'--{prefix}check_dict',
        arg_spaces('k19=s,{"k20":"t"}'),
    ]
    args += [f'--{prefix}check_dict', arg_spaces('{"k21":"u"},k22=v,{"k23":"w"}')]
    args += [f'--{prefix}check_dict', arg_spaces('k24=x,{"k25":"y"},k26=z')]
    args += [f'--{prefix}check_dict', arg_spaces('[k27="x,y",k28="x,y"]', has_quote_comma=True)]
    args += [f'--{prefix}check_dict', arg_spaces('k29="x,y",k30="x,y"', has_quote_comma=True)]
    args += [
        f'--{prefix}check_dict',
        arg_spaces('k31="x,y"', has_quote_comma=True),
        f'--{prefix}check_dict',
        arg_spaces('k32="x,y"', has_quote_comma=True),
    ]
    cfg = CliApp.run(Cfg, cli_args=args)
    expected: dict[str, Any] = {
        'check_dict': {
            'k1': 'a',
            'k2': 'b',
            'k3': 'c',
            'k4': 'd',
            'k5': 'e',
            'k6': 'f',
            'k7': 'g',
            'k8': 'h',
            'k9': 'i',
            'k10': 'j',
            'k11': 'k',
            'k12': 'l',
            'k13': 'm',
            'k14': 'n',
            'k15': 'o',
            'k16': 'p',
            'k17': 'q',
            'k18': 'r',
            'k19': 's',
            'k20': 't',
            'k21': 'u',
            'k22': 'v',
            'k23': 'w',
            'k24': 'x',
            'k25': 'y',
            'k26': 'z',
            'k27': 'x,y',
            'k28': 'x,y',
            'k29': 'x,y',
            'k30': 'x,y',
            'k31': 'x,y',
            'k32': 'x,y',
        }
    }
    if prefix:
        expected = {'check_dict': None, 'child': expected}
    else:
        expected['child'] = None
    assert cfg.model_dump() == expected

    with pytest.raises(SettingsError, match=f'Parsing error encountered for {prefix}check_dict: Mismatched quotes'):
        cfg = CliApp.run(Cfg, cli_args=[f'--{prefix}check_dict', 'k9="i'])

    with pytest.raises(SettingsError, match=f'Parsing error encountered for {prefix}check_dict: Mismatched quotes'):
        cfg = CliApp.run(Cfg, cli_args=[f'--{prefix}check_dict', 'k9=i"'])


def test_cli_union_dict_arg():
    class Cfg(BaseSettings):
        union_str_dict: str | dict[str, Any]

    with pytest.raises(ValidationError) as exc_info:
        args = ['--union_str_dict', 'hello world', '--union_str_dict', 'hello world']
        cfg = CliApp.run(Cfg, cli_args=args)
    assert exc_info.value.errors(include_url=False) == [
        {
            'input': [
                'hello world',
                'hello world',
            ],
            'loc': (
                'union_str_dict',
                'str',
            ),
            'msg': 'Input should be a valid string',
            'type': 'string_type',
        },
        {
            'input': [
                'hello world',
                'hello world',
            ],
            'loc': (
                'union_str_dict',
                'dict[str,any]',
            ),
            'msg': 'Input should be a valid dictionary',
            'type': 'dict_type',
        },
    ]

    args = ['--union_str_dict', 'hello world']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_str_dict': 'hello world'}

    args = ['--union_str_dict', '{"hello": "world"}']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_str_dict': {'hello': 'world'}}

    args = ['--union_str_dict', 'hello=world']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_str_dict': {'hello': 'world'}}

    args = ['--union_str_dict', '"hello=world"']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_str_dict': 'hello=world'}

    class Cfg(BaseSettings):
        union_list_dict: list[str] | dict[str, Any]

    with pytest.raises(ValidationError) as exc_info:
        args = ['--union_list_dict', 'hello,world']
        cfg = CliApp.run(Cfg, cli_args=args)
    assert exc_info.value.errors(include_url=False) == [
        {
            'input': 'hello,world',
            'loc': (
                'union_list_dict',
                'list[str]',
            ),
            'msg': 'Input should be a valid list',
            'type': 'list_type',
        },
        {
            'input': 'hello,world',
            'loc': (
                'union_list_dict',
                'dict[str,any]',
            ),
            'msg': 'Input should be a valid dictionary',
            'type': 'dict_type',
        },
    ]

    args = ['--union_list_dict', 'hello,world', '--union_list_dict', 'hello,world']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_list_dict': ['hello', 'world', 'hello', 'world']}

    args = ['--union_list_dict', '[hello,world]']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_list_dict': ['hello', 'world']}

    args = ['--union_list_dict', '{"hello": "world"}']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_list_dict': {'hello': 'world'}}

    args = ['--union_list_dict', 'hello=world']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_list_dict': {'hello': 'world'}}

    with pytest.raises(ValidationError) as exc_info:
        args = ['--union_list_dict', '"hello=world"']
        cfg = CliApp.run(Cfg, cli_args=args)
    assert exc_info.value.errors(include_url=False) == [
        {
            'input': 'hello=world',
            'loc': (
                'union_list_dict',
                'list[str]',
            ),
            'msg': 'Input should be a valid list',
            'type': 'list_type',
        },
        {
            'input': 'hello=world',
            'loc': (
                'union_list_dict',
                'dict[str,any]',
            ),
            'msg': 'Input should be a valid dictionary',
            'type': 'dict_type',
        },
    ]

    args = ['--union_list_dict', '["hello=world"]']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'union_list_dict': ['hello=world']}


def test_cli_nested_dict_arg():
    class Cfg(BaseSettings):
        check_dict: dict[str, Any]

    args = ['--check_dict', '{"k1":{"a": 1}},{"k2":{"b": 2}}']
    cfg = CliApp.run(Cfg, cli_args=args)
    assert cfg.model_dump() == {'check_dict': {'k1': {'a': 1}, 'k2': {'b': 2}}}

    with pytest.raises(
        SettingsError,
        match=re.escape('Parsing error encountered for check_dict: not enough values to unpack (expected 2, got 1)'),
    ):
        args = ['--check_dict', '{"k1":{"a": 1}},"k2":{"b": 2}}']
        cfg = CliApp.run(Cfg, cli_args=args)

    with pytest.raises(SettingsError, match='Parsing error encountered for check_dict: Missing end delimiter "}"'):
        args = ['--check_dict', '{"k1":{"a": 1}},{"k2":{"b": 2}']
        cfg = CliApp.run(Cfg, cli_args=args)


def test_cli_subcommand_union(capsys, monkeypatch):
    class AlphaCmd(BaseModel):
        """Alpha Help"""

        a: str

    class BetaCmd(BaseModel):
        """Beta Help"""

        b: str

    class GammaCmd(BaseModel):
        """Gamma Help"""

        g: str

    class Root1(BaseSettings):
        """Root Help"""

        subcommand: CliSubCommand[AlphaCmd | BetaCmd | GammaCmd] = Field(description='Field Help')

    alpha = CliApp.run(Root1, cli_args=['AlphaCmd', '-a=alpha'])
    assert get_subcommand(alpha).model_dump() == {'a': 'alpha'}
    assert alpha.model_dump() == {'subcommand': {'a': 'alpha'}}
    beta = CliApp.run(Root1, cli_args=['BetaCmd', '-b=beta'])
    assert get_subcommand(beta).model_dump() == {'b': 'beta'}
    assert beta.model_dump() == {'subcommand': {'b': 'beta'}}
    gamma = CliApp.run(Root1, cli_args=['GammaCmd', '-g=gamma'])
    assert get_subcommand(gamma).model_dump() == {'g': 'gamma'}
    assert gamma.model_dump() == {'subcommand': {'g': 'gamma'}}

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            CliApp.run(Root1)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] {{AlphaCmd,BetaCmd,GammaCmd}} ...

Root Help

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit

subcommands:
  Field Help

  {{AlphaCmd,BetaCmd,GammaCmd}}
    AlphaCmd
    BetaCmd
    GammaCmd
"""
        )

        with pytest.raises(SystemExit):
            Root1(_cli_parse_args=True, _cli_use_class_docs_for_groups=True)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] {{AlphaCmd,BetaCmd,GammaCmd}} ...

Root Help

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit

subcommands:
  Field Help

  {{AlphaCmd,BetaCmd,GammaCmd}}
    AlphaCmd            Alpha Help
    BetaCmd             Beta Help
    GammaCmd            Gamma Help
"""
        )

    class Root2(BaseSettings):
        """Root Help"""

        subcommand: CliSubCommand[AlphaCmd | GammaCmd] = Field(description='Field Help')
        beta: CliSubCommand[BetaCmd] = Field(description='Field Beta Help')

    alpha = CliApp.run(Root2, cli_args=['AlphaCmd', '-a=alpha'])
    assert get_subcommand(alpha).model_dump() == {'a': 'alpha'}
    assert alpha.model_dump() == {'subcommand': {'a': 'alpha'}, 'beta': None}
    beta = CliApp.run(Root2, cli_args=['beta', '-b=beta'])
    assert get_subcommand(beta).model_dump() == {'b': 'beta'}
    assert beta.model_dump() == {'subcommand': None, 'beta': {'b': 'beta'}}
    gamma = CliApp.run(Root2, cli_args=['GammaCmd', '-g=gamma'])
    assert get_subcommand(gamma).model_dump() == {'g': 'gamma'}
    assert gamma.model_dump() == {'subcommand': {'g': 'gamma'}, 'beta': None}

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            CliApp.run(Root2, cli_args=True)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] {{AlphaCmd,GammaCmd,beta}} ...

Root Help

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit

subcommands:
  Field Help

  {{AlphaCmd,GammaCmd,beta}}
    AlphaCmd
    GammaCmd
    beta                Field Beta Help
"""
        )

        with pytest.raises(SystemExit):
            Root2(_cli_parse_args=True, _cli_use_class_docs_for_groups=True)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] {{AlphaCmd,GammaCmd,beta}} ...

Root Help

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit

subcommands:
  Field Help

  {{AlphaCmd,GammaCmd,beta}}
    AlphaCmd            Alpha Help
    GammaCmd            Gamma Help
    beta                Beta Help
"""
        )

    class Root3(BaseSettings):
        """Root Help"""

        beta: CliSubCommand[BetaCmd] = Field(description='Field Beta Help')
        subcommand: CliSubCommand[AlphaCmd | GammaCmd] = Field(description='Field Help')

    alpha = CliApp.run(Root3, cli_args=['AlphaCmd', '-a=alpha'])
    assert get_subcommand(alpha).model_dump() == {'a': 'alpha'}
    assert alpha.model_dump() == {'subcommand': {'a': 'alpha'}, 'beta': None}
    beta = CliApp.run(Root3, cli_args=['beta', '-b=beta'])
    assert get_subcommand(beta).model_dump() == {'b': 'beta'}
    assert beta.model_dump() == {'subcommand': None, 'beta': {'b': 'beta'}}
    gamma = CliApp.run(Root3, cli_args=['GammaCmd', '-g=gamma'])
    assert get_subcommand(gamma).model_dump() == {'g': 'gamma'}
    assert gamma.model_dump() == {'subcommand': {'g': 'gamma'}, 'beta': None}

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            CliApp.run(Root3)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] {{beta,AlphaCmd,GammaCmd}} ...

Root Help

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit

subcommands:
  {{beta,AlphaCmd,GammaCmd}}
    beta                Field Beta Help
    AlphaCmd
    GammaCmd
"""
        )

        with pytest.raises(SystemExit):
            Root3(_cli_parse_args=True, _cli_use_class_docs_for_groups=True)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] {{beta,AlphaCmd,GammaCmd}} ...

Root Help

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit

subcommands:
  {{beta,AlphaCmd,GammaCmd}}
    beta                Beta Help
    AlphaCmd            Alpha Help
    GammaCmd            Gamma Help
"""
        )


def test_cli_subcommand_with_positionals():
    @pydantic_dataclasses.dataclass
    class FooPlugin:
        my_feature: bool = False

    @pydantic_dataclasses.dataclass
    class BarPlugin:
        my_feature: bool = False

    bar = BarPlugin()
    with pytest.raises(SystemExit, match='Error: CLI subcommand is required but no subcommands were found.'):
        get_subcommand(bar)
    with pytest.raises(SettingsError, match='Error: CLI subcommand is required but no subcommands were found.'):
        get_subcommand(bar, cli_exit_on_error=False)

    @pydantic_dataclasses.dataclass
    class Plugins:
        foo: CliSubCommand[FooPlugin]
        bar: CliSubCommand[BarPlugin]

    class Clone(BaseModel):
        repository: CliPositionalArg[str]
        directory: CliPositionalArg[str]
        local: bool = False
        shared: bool = False

    class Init(BaseModel):
        directory: CliPositionalArg[str]
        quiet: bool = False
        bare: bool = False

    class Git(BaseSettings):
        clone: CliSubCommand[Clone]
        init: CliSubCommand[Init]
        plugins: CliSubCommand[Plugins]

    git = CliApp.run(Git, cli_args=[])
    assert git.model_dump() == {
        'clone': None,
        'init': None,
        'plugins': None,
    }
    assert get_subcommand(git, is_required=False) is None
    with pytest.raises(SystemExit, match='Error: CLI subcommand is required {clone, init, plugins}'):
        get_subcommand(git)
    with pytest.raises(SettingsError, match='Error: CLI subcommand is required {clone, init, plugins}'):
        get_subcommand(git, cli_exit_on_error=False)

    git = CliApp.run(Git, cli_args=['init', '--quiet', 'true', 'dir/path'])
    assert git.model_dump() == {
        'clone': None,
        'init': {'directory': 'dir/path', 'quiet': True, 'bare': False},
        'plugins': None,
    }
    assert get_subcommand(git) == git.init
    assert get_subcommand(git, is_required=False) == git.init

    git = CliApp.run(Git, cli_args=['clone', 'repo', '.', '--shared', 'true'])
    assert git.model_dump() == {
        'clone': {'repository': 'repo', 'directory': '.', 'local': False, 'shared': True},
        'init': None,
        'plugins': None,
    }
    assert get_subcommand(git) == git.clone
    assert get_subcommand(git, is_required=False) == git.clone

    git = CliApp.run(Git, cli_args=['plugins', 'bar'])
    assert git.model_dump() == {
        'clone': None,
        'init': None,
        'plugins': {'foo': None, 'bar': {'my_feature': False}},
    }
    assert get_subcommand(git) == git.plugins
    assert get_subcommand(git, is_required=False) == git.plugins
    assert get_subcommand(get_subcommand(git)) == git.plugins.bar
    assert get_subcommand(get_subcommand(git), is_required=False) == git.plugins.bar

    class NotModel: ...

    with pytest.raises(
        SettingsError, match='Error: NotModel is not subclass of BaseModel or pydantic.dataclasses.dataclass'
    ):
        get_subcommand(NotModel())

    class NotSettingsConfigDict(BaseModel):
        model_config = ConfigDict(cli_exit_on_error='not a bool')

    with pytest.raises(SystemExit, match='Error: CLI subcommand is required but no subcommands were found.'):
        get_subcommand(NotSettingsConfigDict())

    with pytest.raises(SettingsError, match='Error: CLI subcommand is required but no subcommands were found.'):
        get_subcommand(NotSettingsConfigDict(), cli_exit_on_error=False)


def test_cli_union_similar_sub_models():
    class ChildA(BaseModel):
        name: str = 'child a'
        diff_a: str = 'child a difference'

    class ChildB(BaseModel):
        name: str = 'child b'
        diff_b: str = 'child b difference'

    class Cfg(BaseSettings):
        child: ChildA | ChildB

    cfg = CliApp.run(Cfg, cli_args=['--child.name', 'new name a', '--child.diff_a', 'new diff a'])
    assert cfg.model_dump() == {'child': {'name': 'new name a', 'diff_a': 'new diff a'}}


def test_cli_optional_positional_arg(env):
    class Main(BaseSettings):
        model_config = SettingsConfigDict(
            cli_parse_args=True,
            cli_enforce_required=True,
        )

        value: CliPositionalArg[int] = 123

    assert CliApp.run(Main, cli_args=[]).model_dump() == {'value': 123}

    env.set('VALUE', '456')
    assert CliApp.run(Main, cli_args=[]).model_dump() == {'value': 456}

    assert CliApp.run(Main, cli_args=['789']).model_dump() == {'value': 789}


def test_cli_variadic_positional_arg(env):
    class MainRequired(BaseSettings):
        model_config = SettingsConfigDict(cli_parse_args=True)

        values: CliPositionalArg[list[int]]

    class MainOptional(MainRequired):
        values: CliPositionalArg[list[int]] = [1, 2, 3]

    assert CliApp.run(MainOptional, cli_args=[]).model_dump() == {'values': [1, 2, 3]}
    with pytest.raises(SettingsError, match='error parsing CLI: the following arguments are required: VALUES'):
        CliApp.run(MainRequired, cli_args=[], cli_exit_on_error=False)

    env.set('VALUES', '[4,5,6]')
    assert CliApp.run(MainOptional, cli_args=[]).model_dump() == {'values': [4, 5, 6]}
    with pytest.raises(SettingsError, match='error parsing CLI: the following arguments are required: VALUES'):
        CliApp.run(MainRequired, cli_args=[], cli_exit_on_error=False)

    assert CliApp.run(MainOptional, cli_args=['7', '8', '9']).model_dump() == {'values': [7, 8, 9]}
    assert CliApp.run(MainRequired, cli_args=['7', '8', '9']).model_dump() == {'values': [7, 8, 9]}


def test_cli_enums(capsys, monkeypatch):
    class Pet(IntEnum):
        dog = 0
        cat = 1
        bird = 2

    class Cfg(BaseSettings):
        pet: Pet = Pet.dog
        union_pet: Pet | int = 43

    cfg = CliApp.run(Cfg, cli_args=['--pet', 'cat', '--union_pet', 'dog'])
    assert cfg.model_dump() == {'pet': Pet.cat, 'union_pet': Pet.dog}

    with pytest.raises(ValidationError) as exc_info:
        CliApp.run(Cfg, cli_args=['--pet', 'rock'])
    assert exc_info.value.errors(include_url=False) == [
        {
            'type': 'enum',
            'loc': ('pet',),
            'msg': 'Input should be 0, 1 or 2',
            'input': 'rock',
            'ctx': {'expected': '0, 1 or 2'},
        }
    ]

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        if PYTHON_3_14:
            if IS_WINDOWS:
                text = '                            [--union_pet {{dog,cat,bird},int}]'
            else:
                text = '                         [--union_pet {{dog,cat,bird},int}]'
        else:
            text = '                  [--union_pet {{dog,cat,bird},int}]'
        with pytest.raises(SystemExit):
            CliApp.run(Cfg)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--pet {{dog,cat,bird}}]
{text}

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit
  --pet {{dog,cat,bird}}  (default: dog)
  --union_pet {{{{dog,cat,bird}},int}}
                        (default: 43)
"""
        )


def test_cli_literals():
    class Cfg(BaseSettings):
        pet: Literal['dog', 'cat', 'bird']

    cfg = CliApp.run(Cfg, cli_args=['--pet', 'cat'])
    assert cfg.model_dump() == {'pet': 'cat'}

    with pytest.raises(ValidationError) as exc_info:
        CliApp.run(Cfg, cli_args=['--pet', 'rock'])
    assert exc_info.value.errors(include_url=False) == [
        {
            'ctx': {'expected': "'dog', 'cat' or 'bird'"},
            'type': 'literal_error',
            'loc': ('pet',),
            'msg': "Input should be 'dog', 'cat' or 'bird'",
            'input': 'rock',
        }
    ]


def test_cli_annotation_exceptions(monkeypatch):
    class SubCmdAlt(BaseModel):
        pass

    class SubCmd(BaseModel):
        pass

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(
            SettingsError, match='CliSubCommand is not outermost annotation for SubCommandNotOutermost.subcmd'
        ):

            class SubCommandNotOutermost(BaseSettings, cli_parse_args=True):
                subcmd: int | CliSubCommand[SubCmd]

            SubCommandNotOutermost()

        with pytest.raises(SettingsError, match='subcommand argument SubCommandHasDefault.subcmd has a default value'):

            class SubCommandHasDefault(BaseSettings, cli_parse_args=True):
                subcmd: CliSubCommand[SubCmd] = SubCmd()

            SubCommandHasDefault()

        with pytest.raises(
            SettingsError,
            match='subcommand argument SubCommandMultipleTypes.subcmd has type not derived from BaseModel',
        ):

            class SubCommandMultipleTypes(BaseSettings, cli_parse_args=True):
                subcmd: CliSubCommand[SubCmd | str]

            SubCommandMultipleTypes()

        with pytest.raises(
            SettingsError, match='subcommand argument SubCommandNotModel.subcmd has type not derived from BaseModel'
        ):

            class SubCommandNotModel(BaseSettings, cli_parse_args=True):
                subcmd: CliSubCommand[str]

            SubCommandNotModel()

        with pytest.raises(
            SettingsError, match='CliPositionalArg is not outermost annotation for PositionalArgNotOutermost.pos_arg'
        ):

            class PositionalArgNotOutermost(BaseSettings, cli_parse_args=True):
                pos_arg: int | CliPositionalArg[str]

            PositionalArgNotOutermost()

        with pytest.raises(
            SettingsError,
            match='MultipleVariadicPositionalArgs has multiple variadic positional arguments: strings, numbers',
        ):

            class MultipleVariadicPositionalArgs(BaseSettings, cli_parse_args=True):
                strings: CliPositionalArg[list[str]]
                numbers: CliPositionalArg[list[int]]

            MultipleVariadicPositionalArgs()

        with pytest.raises(
            SettingsError,
            match='VariadicPositionalArgAndSubCommand has variadic positional arguments and subcommand arguments: strings, sub_cmd',
        ):

            class VariadicPositionalArgAndSubCommand(BaseSettings, cli_parse_args=True):
                strings: CliPositionalArg[list[str]]
                sub_cmd: CliSubCommand[SubCmd]

            VariadicPositionalArgAndSubCommand()

    with pytest.raises(
        SettingsError, match=re.escape("cli_parse_args must be a list or tuple of strings, received <class 'str'>")
    ):

        class InvalidCliParseArgsType(BaseSettings, cli_parse_args='invalid type'):
            val: int

        InvalidCliParseArgsType()

    with pytest.raises(SettingsError, match='CliExplicitFlag argument CliFlagNotBool.flag is not of type bool'):

        class CliFlagNotBool(BaseSettings, cli_parse_args=True):
            flag: CliExplicitFlag[int] = False

        CliFlagNotBool()


@pytest.mark.parametrize('enforce_required', [True, False])
def test_cli_bool_flags(monkeypatch, enforce_required):
    class ExplicitSettings(BaseSettings, cli_enforce_required=enforce_required):
        explicit_req: bool
        explicit_opt: bool = False
        implicit_req: CliImplicitFlag[bool]
        implicit_opt: CliImplicitFlag[bool] = False

    class ImplicitSettings(BaseSettings, cli_implicit_flags=True, cli_enforce_required=enforce_required):
        explicit_req: CliExplicitFlag[bool]
        explicit_opt: CliExplicitFlag[bool] = False
        implicit_req: bool
        implicit_opt: bool = False

    expected = {
        'explicit_req': True,
        'explicit_opt': False,
        'implicit_req': True,
        'implicit_opt': False,
    }

    explicit_settings = CliApp.run(ExplicitSettings, cli_args=['--explicit_req=True', '--implicit_req'])
    assert explicit_settings.model_dump() == expected
    serialized_args = CliApp.serialize(explicit_settings)
    assert serialized_args == ['--explicit_req', 'True', '--implicit_req']
    assert CliApp.run(ExplicitSettings, cli_args=serialized_args).model_dump() == expected

    implicit_settings = CliApp.run(ImplicitSettings, cli_args=['--explicit_req=True', '--implicit_req'])
    assert implicit_settings.model_dump() == expected
    serialized_args = CliApp.serialize(implicit_settings)
    assert serialized_args == ['--explicit_req', 'True', '--implicit_req']
    assert CliApp.run(ImplicitSettings, cli_args=serialized_args).model_dump() == expected

    expected = {
        'explicit_req': False,
        'explicit_opt': False,
        'implicit_req': False,
        'implicit_opt': False,
    }

    explicit_settings = CliApp.run(ExplicitSettings, cli_args=['--explicit_req=False', '--no-implicit_req'])
    assert explicit_settings.model_dump() == expected
    serialized_args = CliApp.serialize(explicit_settings)
    assert serialized_args == ['--explicit_req', 'False', '--no-implicit_req']
    assert CliApp.run(ExplicitSettings, cli_args=serialized_args).model_dump() == expected

    implicit_settings = CliApp.run(ImplicitSettings, cli_args=['--explicit_req=False', '--no-implicit_req'])
    assert implicit_settings.model_dump() == expected
    serialized_args = CliApp.serialize(implicit_settings)
    assert serialized_args == ['--explicit_req', 'False', '--no-implicit_req']
    assert CliApp.run(ImplicitSettings, cli_args=serialized_args).model_dump() == expected


def test_cli_avoid_json(capsys, monkeypatch):
    class SubModel(BaseModel):
        v1: int

    class Settings(BaseSettings):
        sub_model: SubModel

        model_config = SettingsConfigDict(cli_parse_args=True)

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            Settings(_cli_avoid_json=False)

        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--sub_model [JSON]] [--sub_model.v1 int]

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help          show this help message and exit

sub_model options:
  --sub_model [JSON]  set sub_model from JSON string (default: {{}})
  --sub_model.v1 int  (required)
"""
        )

        with pytest.raises(SystemExit):
            Settings(_cli_avoid_json=True)

        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--sub_model.v1 int]

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help          show this help message and exit

sub_model options:
  --sub_model.v1 int  (required)
"""
        )


def test_cli_remove_empty_groups(capsys, monkeypatch):
    class SubModel(BaseModel):
        pass

    class Settings(BaseSettings):
        sub_model: SubModel

        model_config = SettingsConfigDict(cli_parse_args=True)

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            Settings(_cli_avoid_json=False)

        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--sub_model [JSON]]

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help          show this help message and exit

sub_model options:
  --sub_model [JSON]  set sub_model from JSON string (default: {{}})
"""
        )

        with pytest.raises(SystemExit):
            Settings(_cli_avoid_json=True)

        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h]

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help  show this help message and exit
"""
        )


def test_cli_hide_none_type(capsys, monkeypatch):
    class Settings(BaseSettings):
        v0: str | None

        model_config = SettingsConfigDict(cli_parse_args=True)

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            Settings(_cli_hide_none_type=False)

        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--v0 {{str,null}}]

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help       show this help message and exit
  --v0 {{str,null}}  (required)
"""
        )

        with pytest.raises(SystemExit):
            Settings(_cli_hide_none_type=True)

        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--v0 str]

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help  show this help message and exit
  --v0 str    (required)
"""
        )


def test_cli_use_class_docs_for_groups(capsys, monkeypatch):
    class SubModel(BaseModel):
        """The help text from the class docstring"""

        v1: int

    class Settings(BaseSettings):
        """My application help text."""

        sub_model: SubModel = Field(description='The help text from the field description')

        model_config = SettingsConfigDict(cli_parse_args=True)

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            Settings(_cli_use_class_docs_for_groups=False)

        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--sub_model [JSON]] [--sub_model.v1 int]

My application help text.

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help          show this help message and exit

sub_model options:
  The help text from the field description

  --sub_model [JSON]  set sub_model from JSON string (default: {{}})
  --sub_model.v1 int  (required)
"""
        )

        with pytest.raises(SystemExit):
            Settings(_cli_use_class_docs_for_groups=True)

        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] [--sub_model [JSON]] [--sub_model.v1 int]

My application help text.

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help          show this help message and exit

sub_model options:
  The help text from the class docstring

  --sub_model [JSON]  set sub_model from JSON string (default: {{}})
  --sub_model.v1 int  (required)
"""
        )


def test_cli_enforce_required(env):
    class MyRootModel(RootModel[str]):
        root: str

    class Settings(BaseSettings, cli_exit_on_error=False):
        my_required_field: str
        my_root_model_required_field: MyRootModel

    env.set('MY_REQUIRED_FIELD', 'hello from environment')
    env.set('MY_ROOT_MODEL_REQUIRED_FIELD', 'hi from environment')

    assert Settings(_cli_parse_args=[], _cli_enforce_required=False).model_dump() == {
        'my_required_field': 'hello from environment',
        'my_root_model_required_field': 'hi from environment',
    }

    with pytest.raises(
        SettingsError, match='error parsing CLI: the following arguments are required: --my_required_field'
    ):
        Settings(_cli_parse_args=[], _cli_enforce_required=True).model_dump()

    with pytest.raises(
        SettingsError, match='error parsing CLI: the following arguments are required: --my_root_model_required_field'
    ):
        Settings(_cli_parse_args=['--my_required_field', 'hello from cli'], _cli_enforce_required=True).model_dump()


def test_cli_exit_on_error(capsys, monkeypatch):
    class Settings(BaseSettings, cli_parse_args=True): ...

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--bad-arg'])

        with pytest.raises(SystemExit):
            Settings()
        assert (
            sanitize_cli_output(capsys.readouterr().err)
            == """usage: example.py [-h]
example.py: error: unrecognized arguments: --bad-arg
"""
        )

        with pytest.raises(SettingsError, match='error parsing CLI: unrecognized arguments: --bad-arg'):
            CliApp.run(Settings, cli_exit_on_error=False)


def test_cli_ignore_unknown_args():
    class Cfg(BaseSettings, cli_ignore_unknown_args=True):
        this: str = 'hello'
        that: int = 123
        ignored_args: CliUnknownArgs

    cfg = CliApp.run(Cfg, cli_args=['--this=hi', '--that=456'])
    assert cfg.model_dump() == {'this': 'hi', 'that': 456, 'ignored_args': []}

    cfg = CliApp.run(Cfg, cli_args=['not_my_positional_arg', '--not-my-optional-arg=456'])
    assert cfg.model_dump() == {
        'this': 'hello',
        'that': 123,
        'ignored_args': ['not_my_positional_arg', '--not-my-optional-arg=456'],
    }

    cfg = CliApp.run(
        Cfg, cli_args=['not_my_positional_arg', '--not-my-optional-arg=456', '--this=goodbye', '--that=789']
    )
    assert cfg.model_dump() == {
        'this': 'goodbye',
        'that': 789,
        'ignored_args': ['not_my_positional_arg', '--not-my-optional-arg=456'],
    }


def test_cli_flag_prefix_char():
    class Cfg(BaseSettings, cli_flag_prefix_char='+'):
        my_var: str = Field(validation_alias=AliasChoices('m', 'my-var'))

    cfg = CliApp.run(Cfg, cli_args=['++my-var=hello'])
    assert cfg.model_dump() == {'my_var': 'hello'}

    cfg = CliApp.run(Cfg, cli_args=['+m=hello'])
    assert cfg.model_dump() == {'my_var': 'hello'}


@pytest.mark.parametrize('parser_type', [pytest.Parser, argparse.ArgumentParser, CliDummyParser])
@pytest.mark.parametrize('prefix', ['', 'cfg'])
def test_cli_user_settings_source(parser_type, prefix):
    class Cfg(BaseSettings):
        pet: Literal['dog', 'cat', 'bird'] = 'bird'

    if parser_type is pytest.Parser:
        parser = pytest.Parser(_ispytest=True)
        parse_args = parser.parse
        add_arg = parser.addoption
        cli_cfg_settings = CliSettingsSource(
            Cfg,
            cli_prefix=prefix,
            root_parser=parser,
            parse_args_method=pytest.Parser.parse,
            add_argument_method=pytest.Parser.addoption,
            add_argument_group_method=pytest.Parser.getgroup,
            add_parser_method=None,
            add_subparsers_method=None,
            formatter_class=None,
        )
    elif parser_type is CliDummyParser:
        parser = CliDummyParser()
        parse_args = parser.parse_args
        add_arg = parser.add_argument
        cli_cfg_settings = CliSettingsSource(
            Cfg,
            cli_prefix=prefix,
            root_parser=parser,
            parse_args_method=CliDummyParser.parse_args,
            add_argument_method=CliDummyParser.add_argument,
            add_argument_group_method=CliDummyParser.add_argument_group,
            add_parser_method=CliDummySubParsers.add_parser,
            add_subparsers_method=CliDummyParser.add_subparsers,
        )
    else:
        parser = argparse.ArgumentParser()
        parse_args = parser.parse_args
        add_arg = parser.add_argument
        cli_cfg_settings = CliSettingsSource(Cfg, cli_prefix=prefix, root_parser=parser)

    add_arg('--fruit', choices=['pear', 'kiwi', 'lime'])
    add_arg('--num-list', action='append', type=int)
    add_arg('--num', type=int)

    args = ['--fruit', 'pear', '--num', '0', '--num-list', '1', '--num-list', '2', '--num-list', '3']
    parsed_args = parse_args(args)
    assert CliApp.run(Cfg, cli_args=parsed_args, cli_settings_source=cli_cfg_settings).model_dump() == {'pet': 'bird'}
    assert CliApp.run(Cfg, cli_args=args, cli_settings_source=cli_cfg_settings).model_dump() == {'pet': 'bird'}
    assert Cfg(_cli_settings_source=cli_cfg_settings(parsed_args=parsed_args)).model_dump() == {'pet': 'bird'}
    assert Cfg(_cli_settings_source=cli_cfg_settings(args=args)).model_dump() == {'pet': 'bird'}
    assert Cfg(_cli_settings_source=cli_cfg_settings(args=False)).model_dump() == {'pet': 'bird'}

    arg_prefix = f'{prefix}.' if prefix else ''
    args = [
        '--fruit',
        'kiwi',
        '--num',
        '0',
        '--num-list',
        '1',
        '--num-list',
        '2',
        '--num-list',
        '3',
        f'--{arg_prefix}pet',
        'dog',
    ]
    parsed_args = parse_args(args)
    assert CliApp.run(Cfg, cli_args=parsed_args, cli_settings_source=cli_cfg_settings).model_dump() == {'pet': 'dog'}
    assert CliApp.run(Cfg, cli_args=args, cli_settings_source=cli_cfg_settings).model_dump() == {'pet': 'dog'}
    assert Cfg(_cli_settings_source=cli_cfg_settings(parsed_args=parsed_args)).model_dump() == {'pet': 'dog'}
    assert Cfg(_cli_settings_source=cli_cfg_settings(args=args)).model_dump() == {'pet': 'dog'}
    assert Cfg(_cli_settings_source=cli_cfg_settings(args=False)).model_dump() == {'pet': 'bird'}

    parsed_args = parse_args(
        [
            '--fruit',
            'kiwi',
            '--num',
            '0',
            '--num-list',
            '1',
            '--num-list',
            '2',
            '--num-list',
            '3',
            f'--{arg_prefix}pet',
            'cat',
        ]
    )
    assert CliApp.run(Cfg, cli_args=vars(parsed_args), cli_settings_source=cli_cfg_settings).model_dump() == {
        'pet': 'cat'
    }
    assert Cfg(_cli_settings_source=cli_cfg_settings(parsed_args=vars(parsed_args))).model_dump() == {'pet': 'cat'}
    assert Cfg(_cli_settings_source=cli_cfg_settings(args=False)).model_dump() == {'pet': 'bird'}


@pytest.mark.parametrize('prefix', ['', 'cfg'])
def test_cli_dummy_user_settings_with_subcommand(prefix):
    class DogCommands(BaseModel):
        name: str = 'Bob'
        command: Literal['roll', 'bark', 'sit'] = 'sit'

    class Cfg(BaseSettings):
        pet: Literal['dog', 'cat', 'bird'] = 'bird'
        command: CliSubCommand[DogCommands]

    parser = CliDummyParser()
    cli_cfg_settings = CliSettingsSource(
        Cfg,
        root_parser=parser,
        cli_prefix=prefix,
        parse_args_method=CliDummyParser.parse_args,
        add_argument_method=CliDummyParser.add_argument,
        add_argument_group_method=CliDummyParser.add_argument_group,
        add_parser_method=CliDummySubParsers.add_parser,
        add_subparsers_method=CliDummyParser.add_subparsers,
    )

    parser.add_argument('--fruit', choices=['pear', 'kiwi', 'lime'])

    args = ['--fruit', 'pear']
    parsed_args = parser.parse_args(args)
    assert CliApp.run(Cfg, cli_args=parsed_args, cli_settings_source=cli_cfg_settings).model_dump() == {
        'pet': 'bird',
        'command': None,
    }
    assert CliApp.run(Cfg, cli_args=args, cli_settings_source=cli_cfg_settings).model_dump() == {
        'pet': 'bird',
        'command': None,
    }

    arg_prefix = f'{prefix}.' if prefix else ''
    args = ['--fruit', 'kiwi', f'--{arg_prefix}pet', 'dog']
    parsed_args = parser.parse_args(args)
    assert CliApp.run(Cfg, cli_args=parsed_args, cli_settings_source=cli_cfg_settings).model_dump() == {
        'pet': 'dog',
        'command': None,
    }
    assert CliApp.run(Cfg, cli_args=args, cli_settings_source=cli_cfg_settings).model_dump() == {
        'pet': 'dog',
        'command': None,
    }

    parsed_args = parser.parse_args(['--fruit', 'kiwi', f'--{arg_prefix}pet', 'cat'])
    assert CliApp.run(Cfg, cli_args=vars(parsed_args), cli_settings_source=cli_cfg_settings).model_dump() == {
        'pet': 'cat',
        'command': None,
    }

    args = ['--fruit', 'kiwi', f'--{arg_prefix}pet', 'dog', 'command', '--name', 'ralph', '--command', 'roll']
    parsed_args = parser.parse_args(args)
    assert CliApp.run(Cfg, cli_args=vars(parsed_args), cli_settings_source=cli_cfg_settings).model_dump() == {
        'pet': 'dog',
        'command': {'name': 'ralph', 'command': 'roll'},
    }
    assert CliApp.run(Cfg, cli_args=args, cli_settings_source=cli_cfg_settings).model_dump() == {
        'pet': 'dog',
        'command': {'name': 'ralph', 'command': 'roll'},
    }


def test_cli_user_settings_source_exceptions():
    class Cfg(BaseSettings):
        pet: Literal['dog', 'cat', 'bird'] = 'bird'

    with pytest.raises(SettingsError, match='`args` and `parsed_args` are mutually exclusive'):
        args = ['--pet', 'dog']
        parsed_args = {'pet': 'dog'}
        cli_cfg_settings = CliSettingsSource(Cfg)
        Cfg(_cli_settings_source=cli_cfg_settings(args=args, parsed_args=parsed_args))

    with pytest.raises(SettingsError, match='CLI settings source prefix is invalid: .cfg'):
        CliSettingsSource(Cfg, cli_prefix='.cfg')

    with pytest.raises(SettingsError, match='CLI settings source prefix is invalid: cfg.'):
        CliSettingsSource(Cfg, cli_prefix='cfg.')

    with pytest.raises(SettingsError, match='CLI settings source prefix is invalid: 123'):
        CliSettingsSource(Cfg, cli_prefix='123')

    class Food(BaseModel):
        fruit: FruitsEnum = FruitsEnum.kiwi

    class CfgWithSubCommand(BaseSettings):
        pet: Literal['dog', 'cat', 'bird'] = 'bird'
        food: CliSubCommand[Food]

    with pytest.raises(
        SettingsError,
        match='cannot connect CLI settings source root parser: add_subparsers_method is set to `None` but is needed for connecting',
    ):
        CliSettingsSource(CfgWithSubCommand, add_subparsers_method=None)


@pytest.mark.parametrize(
    'value,expected',
    [
        (str, 'str'),
        ('foobar', 'str'),
        ('SomeForwardRefString', 'str'),  # included to document current behavior; could be changed
        (List['SomeForwardRef'], "List[ForwardRef('SomeForwardRef')]"),  # noqa: F821, UP006
        (str | int, '{str,int}'),
        (list, 'list'),
        (List, 'List'),  # noqa: UP006
        ([1, 2, 3], 'list'),
        (List[Dict[str, int]], 'List[Dict[str,int]]'),  # noqa: UP006
        (Tuple[str, int, float], 'Tuple[str,int,float]'),  # noqa: UP006
        (Tuple[str, ...], 'Tuple[str,...]'),  # noqa: UP006
        (int | List[str] | Tuple[str, int], '{int,List[str],Tuple[str,int]}'),  # noqa: UP006
        (foobar, 'foobar'),
        (LoggedVar, 'LoggedVar'),
        (LoggedVar(), 'LoggedVar'),
        (Representation(), 'Representation()'),
        (typing.Literal[1, 2, 3], '{1,2,3}'),
        (typing_extensions.Literal[1, 2, 3], '{1,2,3}'),
        (typing.Literal['a', 'b', 'c'], '{a,b,c}'),
        (typing_extensions.Literal['a', 'b', 'c'], '{a,b,c}'),
        (SimpleSettings, 'JSON'),
        (SimpleSettings | SettingWithIgnoreEmpty, 'JSON'),
        (Union[SimpleSettings, str, SettingWithIgnoreEmpty], '{JSON,str}'),  # noqa: UP007
        (Union[str, SimpleSettings, SettingWithIgnoreEmpty], '{str,JSON}'),  # noqa: UP007
        (Annotated[SimpleSettings, 'annotation'], 'JSON'),
        (DirectoryPath, 'Path'),
        (FruitsEnum, '{pear,kiwi,lime}'),
        (time.time_ns, 'time_ns'),
        (foobar, 'foobar'),
        (CliDummyParser.add_argument, 'CliDummyParser.add_argument'),
        (lambda: str | int, '{str,int}'),
        (lambda: list[int], 'list[int]'),
        (lambda: List[int], 'List[int]'),  # noqa: UP006
        (lambda: list[dict[str, int]], 'list[dict[str,int]]'),
        (lambda: list[str | int], 'list[{str,int}]'),
        (lambda: list[str | int], 'list[{str,int}]'),
        (lambda: LoggedVar[int], 'LoggedVar[int]'),
        (lambda: LoggedVar[Dict[int, str]], 'LoggedVar[Dict[int,str]]'),  # noqa: UP006
    ],
)
@pytest.mark.parametrize('hide_none_type', [True, False])
def test_cli_metavar_format(hide_none_type, value, expected):
    if callable(value) and value.__name__ == '<lambda>':
        value = value()

    cli_settings = CliSettingsSource(SimpleSettings, cli_hide_none_type=hide_none_type)
    if hide_none_type:
        if value == [1, 2, 3] or isinstance(value, LoggedVar) or isinstance(value, Representation):
            pytest.skip()
        if value in ('foobar', 'SomeForwardRefString'):
            expected = f"ForwardRef('{value}')"  # forward ref implicit cast
        if typing_extensions.get_origin(value) is Union:
            args = typing_extensions.get_args(value)
            value = Union[args + (None,) if args else (value, None)]  # noqa: UP007
        else:
            value = Union[(value, None)]  # noqa: UP007
    assert cli_settings._metavar_format(value) == expected


@pytest.mark.skipif(sys.version_info < (3, 12), reason='requires python 3.12 or higher')
def test_cli_metavar_format_type_alias_312():
    exec(
        """
type TypeAliasInt = int
assert CliSettingsSource(SimpleSettings)._metavar_format(TypeAliasInt) == 'TypeAliasInt'
"""
    )


def test_cli_app():
    class Init(BaseModel):
        directory: CliPositionalArg[str]

        def cli_cmd(self) -> None:
            self.directory = 'ran Init.cli_cmd'

        def alt_cmd(self) -> None:
            self.directory = 'ran Init.alt_cmd'

    class Clone(BaseModel):
        repository: CliPositionalArg[str]
        directory: CliPositionalArg[str]

        def cli_cmd(self) -> None:
            self.repository = 'ran Clone.cli_cmd'

        def alt_cmd(self) -> None:
            self.repository = 'ran Clone.alt_cmd'

    class Git(BaseModel):
        clone: CliSubCommand[Clone]
        init: CliSubCommand[Init]

        def cli_cmd(self) -> None:
            CliApp.run_subcommand(self)

        def alt_cmd(self) -> None:
            CliApp.run_subcommand(self, cli_cmd_method_name='alt_cmd')

    assert CliApp.run(Git, cli_args=['init', 'dir']).model_dump() == {
        'clone': None,
        'init': {'directory': 'ran Init.cli_cmd'},
    }
    assert CliApp.run(Git, cli_args=['init', 'dir'], cli_cmd_method_name='alt_cmd').model_dump() == {
        'clone': None,
        'init': {'directory': 'ran Init.alt_cmd'},
    }
    assert CliApp.run(Git, cli_args=['clone', 'repo', 'dir']).model_dump() == {
        'clone': {'repository': 'ran Clone.cli_cmd', 'directory': 'dir'},
        'init': None,
    }
    assert CliApp.run(Git, cli_args=['clone', 'repo', 'dir'], cli_cmd_method_name='alt_cmd').model_dump() == {
        'clone': {'repository': 'ran Clone.alt_cmd', 'directory': 'dir'},
        'init': None,
    }


def test_cli_app_async_method_no_existing_loop():
    class Command(BaseSettings):
        called: bool = False

        async def cli_cmd(self) -> None:
            self.called = True

    assert CliApp.run(Command, cli_args=[]).called


def test_cli_app_async_method_with_existing_loop():
    class Command(BaseSettings):
        called: bool = False

        async def cli_cmd(self) -> None:
            self.called = True

    async def run_as_coro():
        return CliApp.run(Command, cli_args=[])

    assert asyncio.run(run_as_coro()).called


def test_cli_app_exceptions():
    with pytest.raises(
        SettingsError, match='Error: NotPydanticModel is not subclass of BaseModel or pydantic.dataclasses.dataclass'
    ):

        class NotPydanticModel: ...

        CliApp.run(NotPydanticModel)

    with pytest.raises(
        SettingsError,
        match=re.escape('Error: `cli_args` must be list[str] or None when `cli_settings_source` is not used'),
    ):

        class Cfg(BaseModel): ...

        CliApp.run(Cfg, cli_args={'my_arg': 'hello'})

    with pytest.raises(SettingsError, match='Error: Child class is missing cli_cmd entrypoint'):

        class Child(BaseModel):
            val: str

        class Root(BaseModel):
            child: CliSubCommand[Child]

            def cli_cmd(self) -> None:
                CliApp.run_subcommand(self)

        CliApp.run(Root, cli_args=['child', '--val=hello'])


def test_cli_suppress(capsys, monkeypatch):
    class DeepHiddenSubModel(BaseModel):
        deep_hidden_a: int
        deep_hidden_b: int

    class HiddenSubModel(BaseModel):
        hidden_a: int
        hidden_b: int
        deep_hidden_obj: DeepHiddenSubModel = Field(description='deep_hidden_obj description')

    class SubModel(BaseModel):
        visible_a: int
        visible_b: int
        deep_hidden_obj: CliSuppress[DeepHiddenSubModel] = Field(description='deep_hidden_obj description')

    class Settings(BaseSettings, cli_parse_args=True):
        field_a: CliSuppress[int] = 0
        field_b: str = Field(default='hi', description=CLI_SUPPRESS)
        hidden_obj: CliSuppress[HiddenSubModel] = Field(description='hidden_obj description')
        visible_obj: SubModel

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])

        with pytest.raises(SystemExit):
            CliApp.run(Settings)

        if PYTHON_3_14:
            if IS_WINDOWS:
                text = """usage: example.py [-h] [--visible_obj [JSON]]
                            [--visible_obj.visible_a int]
                            [--visible_obj.visible_b int]"""
            else:
                text = """usage: example.py [-h] [--visible_obj [JSON]]
                         [--visible_obj.visible_a int]
                         [--visible_obj.visible_b int]"""

        else:
            text = """usage: example.py [-h] [--visible_obj [JSON]] [--visible_obj.visible_a int]
                  [--visible_obj.visible_b int]"""
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""{text}

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit

visible_obj options:
  --visible_obj [JSON]  set visible_obj from JSON string (default: {{}})
  --visible_obj.visible_a int
                        (required)
  --visible_obj.visible_b int
                        (required)
"""
        )


def test_cli_mutually_exclusive_group(capsys, monkeypatch):
    class Circle(CliMutuallyExclusiveGroup):
        radius: float | None = 21
        diameter: float | None = 22
        perimeter: float | None = 23

    class Settings(BaseModel):
        circle_optional: Circle = Circle(radius=None, diameter=None, perimeter=24)
        circle_required: Circle

    CliApp.run(Settings, cli_args=['--circle-required.radius=1', '--circle-optional.radius=1']).model_dump() == {
        'circle_optional': {'radius': 1, 'diameter': 22, 'perimeter': 24},
        'circle_required': {'radius': 1, 'diameter': 22, 'perimeter': 23},
    }

    with pytest.raises(SystemExit):
        CliApp.run(Settings, cli_args=['--circle-required.radius=1', '--circle-required.diameter=2'])
    assert (
        'error: argument --circle-required.diameter: not allowed with argument --circle-required.radius'
        in capsys.readouterr().err
    )

    with pytest.raises(SystemExit):
        CliApp.run(
            Settings,
            cli_args=['--circle-required.radius=1', '--circle-optional.radius=1', '--circle-optional.diameter=2'],
        )
    assert (
        'error: argument --circle-optional.diameter: not allowed with argument --circle-optional.radius'
        in capsys.readouterr().err
    )

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])
        with pytest.raises(SystemExit):
            CliApp.run(Settings)
        if PYTHON_3_14:
            if IS_WINDOWS:
                usage = """usage: example.py [-h] [--circle-optional.radius float |
                            --circle-optional.diameter float |
                            --circle-optional.perimeter float]
                            (--circle-required.radius float |
                            --circle-required.diameter float |
                            --circle-required.perimeter float)"""
            else:
                usage = """usage: example.py [-h] [--circle-optional.radius float |
                         --circle-optional.diameter float |
                         --circle-optional.perimeter float]
                         (--circle-required.radius float |
                         --circle-required.diameter float |
                         --circle-required.perimeter float)"""
        elif sys.version_info >= (3, 13):
            usage = """usage: example.py [-h] [--circle-optional.radius float |
                  --circle-optional.diameter float |
                  --circle-optional.perimeter float]
                  (--circle-required.radius float |
                  --circle-required.diameter float |
                  --circle-required.perimeter float)"""
        else:
            usage = """usage: example.py [-h]
                  [--circle-optional.radius float | --circle-optional.diameter float | --circle-optional.perimeter float]
                  (--circle-required.radius float | --circle-required.diameter float | --circle-required.perimeter float)"""
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""{usage}

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit

circle-optional options (mutually exclusive):
  --circle-optional.radius float
                        (default: None)
  --circle-optional.diameter float
                        (default: None)
  --circle-optional.perimeter float
                        (default: 24.0)

circle-required options (mutually exclusive):
  --circle-required.radius float
                        (default: 21)
  --circle-required.diameter float
                        (default: 22)
  --circle-required.perimeter float
                        (default: 23)
"""
        )


def test_cli_mutually_exclusive_group_exceptions():
    class Circle(CliMutuallyExclusiveGroup):
        radius: float | None = 21
        diameter: float | None = 22
        perimeter: float | None = 23

    class Settings(BaseSettings):
        circle: Circle

    parser = CliDummyParser()
    with pytest.raises(
        SettingsError,
        match='cannot connect CLI settings source root parser: group object is missing add_mutually_exclusive_group but is needed for connecting',
    ):
        CliSettingsSource(
            Settings,
            root_parser=parser,
            parse_args_method=CliDummyParser.parse_args,
            add_argument_method=CliDummyParser.add_argument,
            add_argument_group_method=CliDummyParser.add_argument_group,
            add_parser_method=CliDummySubParsers.add_parser,
            add_subparsers_method=CliDummyParser.add_subparsers,
        )

    class SubModel(BaseModel):
        pass

    class SettingsInvalidUnion(BaseSettings):
        union: Circle | SubModel

    with pytest.raises(SettingsError, match='cannot use union with CliMutuallyExclusiveGroup'):
        CliApp.run(SettingsInvalidUnion)

    class CircleInvalidSubModel(Circle):
        square: SubModel | None = None

    class SettingsInvalidOptSubModel(BaseModel):
        circle: CircleInvalidSubModel = CircleInvalidSubModel()

    class SettingsInvalidReqSubModel(BaseModel):
        circle: CircleInvalidSubModel

    for settings in [SettingsInvalidOptSubModel, SettingsInvalidReqSubModel]:
        with pytest.raises(SettingsError, match='cannot have nested models in a CliMutuallyExclusiveGroup'):
            CliApp.run(settings)

    class CircleRequiredField(Circle):
        length: float

    class SettingsOptCircleReqField(BaseModel):
        circle: CircleRequiredField = CircleRequiredField(length=2)

    assert CliApp.run(SettingsOptCircleReqField, cli_args=[]).model_dump() == {
        'circle': {'diameter': 22.0, 'length': 2.0, 'perimeter': 23.0, 'radius': 21.0}
    }

    class SettingsInvalidReqCircleReqField(BaseModel):
        circle: CircleRequiredField

    with pytest.raises(ValueError, match='mutually exclusive arguments must be optional'):
        CliApp.run(SettingsInvalidReqCircleReqField)


def test_cli_invalid_abbrev():
    class MySettings(BaseSettings):
        bacon: str = ''
        badger: str = ''

    with pytest.raises(
        SettingsError,
        match='error parsing CLI: unrecognized arguments: --bac cli abbrev are invalid for internal parser',
    ):
        CliApp.run(
            MySettings, cli_args=['--bac', 'cli abbrev are invalid for internal parser'], cli_exit_on_error=False
        )


def test_cli_subcommand_invalid_abbrev():
    class Child(BaseModel):
        bacon: str = ''
        badger: str = ''

    class MySettings(BaseSettings):
        child: CliSubCommand[Child]

    with pytest.raises(
        SettingsError,
        match='error parsing CLI: unrecognized arguments: --bac cli abbrev are invalid for internal parser',
    ):
        CliApp.run(
            MySettings,
            cli_args=['child', '--bac', 'cli abbrev are invalid for internal parser'],
            cli_exit_on_error=False,
        )


def test_cli_submodels_strip_annotated():
    class PolyA(BaseModel):
        a: int = 1
        type: Literal['a'] = 'a'

    class PolyB(BaseModel):
        b: str = '2'
        type: Literal['b'] = 'b'

    def _get_type(model: BaseModel | dict) -> str:
        if isinstance(model, dict):
            return model.get('type', 'a')
        return model.type  # type: ignore

    Poly = Annotated[Annotated[PolyA, Tag('a')] | Annotated[PolyB, Tag('b')], Discriminator(_get_type)]

    class WithUnion(BaseSettings):
        poly: Poly

    assert CliApp.run(WithUnion, ['--poly.type=a']).model_dump() == {'poly': {'a': 1, 'type': 'a'}}


def test_cli_kebab_case(capsys, monkeypatch):
    class DeepSubModel(BaseModel):
        deep_pos_arg: CliPositionalArg[str]
        deep_arg: str

    class SubModel(BaseModel):
        sub_subcmd: CliSubCommand[DeepSubModel]
        sub_other_subcmd: CliSubCommand[DeepSubModel]
        sub_arg: str

    class Root(BaseModel):
        root_subcmd: CliSubCommand[SubModel]
        other_subcmd: CliSubCommand[SubModel]
        root_arg: str

    root = CliApp.run(
        Root,
        cli_args=[
            '--root-arg=hi',
            'root-subcmd',
            '--sub-arg=hello',
            'sub-subcmd',
            'hey',
            '--deep-arg=bye',
        ],
    )
    assert root.model_dump() == {
        'root_arg': 'hi',
        'other_subcmd': None,
        'root_subcmd': {
            'sub_arg': 'hello',
            'sub_subcmd': {'deep_pos_arg': 'hey', 'deep_arg': 'bye'},
            'sub_other_subcmd': None,
        },
    }

    serialized_cli_args = CliApp.serialize(root)
    assert serialized_cli_args == [
        '--root-arg',
        'hi',
        'root-subcmd',
        '--sub-arg',
        'hello',
        'sub-subcmd',
        '--deep-arg',
        'bye',
        'hey',
    ]

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--help'])
        with pytest.raises(SystemExit):
            CliApp.run(Root)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""usage: example.py [-h] --root-arg str {{root-subcmd,other-subcmd}} ...

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit
  --root-arg str        (required)

subcommands:
  {{root-subcmd,other-subcmd}}
    root-subcmd
    other-subcmd
"""
        )

        if PYTHON_3_14:
            if IS_WINDOWS:
                usage = """usage: example.py root-subcmd [-h] --sub-arg str
                                        {sub-subcmd,sub-other-subcmd} ..."""
            else:
                usage = """usage: example.py root-subcmd [-h] --sub-arg str
                                     {sub-subcmd,sub-other-subcmd} ..."""
        else:
            usage = """usage: example.py root-subcmd [-h] --sub-arg str
                              {sub-subcmd,sub-other-subcmd} ..."""
        m.setattr(sys, 'argv', ['example.py', 'root-subcmd', '--help'])
        with pytest.raises(SystemExit):
            CliApp.run(Root)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""{usage}

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help            show this help message and exit
  --sub-arg str         (required)

subcommands:
  {{sub-subcmd,sub-other-subcmd}}
    sub-subcmd
    sub-other-subcmd
"""
        )

        if PYTHON_3_14:
            if IS_WINDOWS:
                usage = """usage: example.py root-subcmd sub-subcmd [-h] --deep-arg str
                                                   DEEP-POS-ARG"""
            else:
                usage = """usage: example.py root-subcmd sub-subcmd [-h] --deep-arg str
                                                DEEP-POS-ARG"""
        else:
            usage = """usage: example.py root-subcmd sub-subcmd [-h] --deep-arg str DEEP-POS-ARG"""
        m.setattr(sys, 'argv', ['example.py', 'root-subcmd', 'sub-subcmd', '--help'])
        with pytest.raises(SystemExit):
            CliApp.run(Root)
        assert (
            sanitize_cli_output(capsys.readouterr().out)
            == f"""{usage}

positional arguments:
  DEEP-POS-ARG

{ARGPARSE_OPTIONS_TEXT}:
  -h, --help      show this help message and exit
  --deep-arg str  (required)
"""
        )


def test_cli_kebab_case_enums():
    class Example1(IntEnum):
        example_a = 0
        example_b = 1

    class Example2(IntEnum):
        example_c = 2
        example_d = 3

    class SettingsNoEnum(BaseSettings):
        model_config = SettingsConfigDict(cli_kebab_case='no_enums')
        example: Example1 | Example2
        mybool: bool

    class SettingsAll(BaseSettings):
        model_config = SettingsConfigDict(cli_kebab_case='all')
        example: Example1 | Example2
        mybool: bool

    assert CliApp.run(
        SettingsNoEnum,
        cli_args=['--example', 'example_a', '--mybool=true'],
    ).model_dump() == {'example': Example1.example_a, 'mybool': True}

    assert CliApp.run(SettingsAll, cli_args=['--example', 'example-c', '--mybool=true']).model_dump() == {
        'example': Example2.example_c,
        'mybool': True,
    }

    with pytest.raises(ValueError, match='Input should be kebab-case "example-a", not "example_a"'):
        CliApp.run(SettingsAll, cli_args=['--example', 'example_a', '--mybool=true'])


def test_cli_with_unbalanced_brackets_in_json_string():
    class StrToStrDictOptions(BaseSettings):
        nested: dict[str, str]

    assert CliApp.run(StrToStrDictOptions, cli_args=['--nested={"test": "{"}']).model_dump() == {
        'nested': {'test': '{'}
    }
    assert CliApp.run(StrToStrDictOptions, cli_args=['--nested={"test": "}"}']).model_dump() == {
        'nested': {'test': '}'}
    }
    assert CliApp.run(StrToStrDictOptions, cli_args=['--nested={"test": "["}']).model_dump() == {
        'nested': {'test': '['}
    }
    assert CliApp.run(StrToStrDictOptions, cli_args=['--nested={"test": "]"}']).model_dump() == {
        'nested': {'test': ']'}
    }

    class StrToListDictOptions(BaseSettings):
        nested: dict[str, list[str]]

    assert CliApp.run(StrToListDictOptions, cli_args=['--nested={"test": ["{"]}']).model_dump() == {
        'nested': {'test': ['{']}
    }
    assert CliApp.run(StrToListDictOptions, cli_args=['--nested={"test": ["}"]}']).model_dump() == {
        'nested': {'test': ['}']}
    }
    assert CliApp.run(StrToListDictOptions, cli_args=['--nested={"test": ["["]}']).model_dump() == {
        'nested': {'test': ['[']}
    }
    assert CliApp.run(StrToListDictOptions, cli_args=['--nested={"test": ["]"]}']).model_dump() == {
        'nested': {'test': [']']}
    }


def test_cli_json_optional_default():
    class Nested(BaseModel):
        foo: int = 1
        bar: int = 2

    class Options(BaseSettings):
        nested: Nested = Nested(foo=3, bar=4)

    assert CliApp.run(Options, cli_args=[]).model_dump() == {'nested': {'foo': 3, 'bar': 4}}
    assert CliApp.run(Options, cli_args=['--nested']).model_dump() == {'nested': {'foo': 1, 'bar': 2}}
    assert CliApp.run(Options, cli_args=['--nested={}']).model_dump() == {'nested': {'foo': 1, 'bar': 2}}
    assert CliApp.run(Options, cli_args=['--nested.foo=5']).model_dump() == {'nested': {'foo': 5, 'bar': 2}}


def test_cli_parse_args_from_model_config_is_respected_with_settings_customise_sources(
    monkeypatch: pytest.MonkeyPatch,
):
    class MySettings(BaseSettings):
        model_config = SettingsConfigDict(cli_parse_args=True)

        foo: str

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ) -> tuple[PydanticBaseSettingsSource, ...]:
            return (CliSettingsSource(settings_cls),)

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--foo', 'bar'])

        cfg = CliApp.run(MySettings)

        assert cfg.model_dump() == {'foo': 'bar'}


def test_cli_shortcuts_on_flat_object():
    class Settings(BaseSettings):
        option: str = Field(default='foo')
        list_option: str = Field(default='fizz')

        model_config = SettingsConfigDict(cli_shortcuts={'option': 'option2', 'list_option': ['list_option2']})

    assert CliApp.run(Settings, cli_args=['--option2', 'bar', '--list_option2', 'buzz']).model_dump() == {
        'option': 'bar',
        'list_option': 'buzz',
    }


def test_cli_shortcuts_on_nested_object():
    class TwiceNested(BaseModel):
        option: str = Field(default='foo')

    class Nested(BaseModel):
        twice_nested_option: TwiceNested = TwiceNested()
        option: str = Field(default='foo')

    class Settings(BaseSettings):
        nested: Nested = Nested()

        model_config = SettingsConfigDict(
            cli_shortcuts={'nested.option': 'option2', 'nested.twice_nested_option.option': 'twice_nested_option'}
        )

    assert CliApp.run(Settings, cli_args=['--option2', 'bar', '--twice_nested_option', 'baz']).model_dump() == {
        'nested': {'option': 'bar', 'twice_nested_option': {'option': 'baz'}}
    }


def test_cli_shortcuts_alias_collision_applies_to_first_target_field():
    class Nested(BaseModel):
        option: str = Field(default='foo')

    class Settings(BaseSettings):
        nested: Nested = Nested()
        option2: str = Field(default='foo2')

        model_config = SettingsConfigDict(cli_shortcuts={'option2': 'abc', 'nested.option': 'abc'})

    assert CliApp.run(Settings, cli_args=['--abc', 'bar']).model_dump() == {
        'nested': {'option': 'bar'},
        'option2': 'foo2',
    }


def test_cli_serialize_positional_args():
    class Nested(BaseModel):
        deep: CliPositionalArg[int]

    class Cfg(BaseSettings):
        top: CliPositionalArg[int]

        variadic: CliPositionalArg[list[int]]

        nested_0: Nested

        nested_1: Nested

    cfg = CliApp.run(Cfg, cli_args=['0', '1', '2', '3', '4', '5'])
    assert cfg.model_dump() == {
        'top': 0,
        'variadic': [
            1,
            2,
            3,
        ],
        'nested_0': {
            'deep': 4,
        },
        'nested_1': {
            'deep': 5,
        },
    }

    serialized_cli_args = CliApp.serialize(cfg)
    assert serialized_cli_args == ['0', '1', '2', '3', '4', '5']
    assert CliApp.run(Cfg, cli_args=serialized_cli_args).model_dump() == cfg.model_dump()


def test_cli_app_with_separate_parser(monkeypatch):
    class Cfg(BaseSettings):
        model_config = SettingsConfigDict(cli_parse_args=True)
        pet: Literal['dog', 'cat', 'bird']

    parser = argparse.ArgumentParser()

    # The actual parsing of command line argument should not happen here.
    cli_settings = CliSettingsSource(Cfg, root_parser=parser)

    parser.add_argument('-e', '--extra', dest='extra', default=0, action='count')

    with monkeypatch.context() as m:
        m.setattr(sys, 'argv', ['example.py', '--pet', 'dog', '-eeee'])

        parsed_args = parser.parse_args()

    assert parsed_args.extra == 4
    # With parsed arguments passed to CliApp.run, the parser should not need to be called again.
    assert CliApp.run(Cfg, cli_args=parsed_args, cli_settings_source=cli_settings).model_dump() == {'pet': 'dog'}


def test_cli_serialize_non_default_values():
    class Cfg(BaseSettings):
        default_val: int = 123
        non_default_val: int

    cfg = Cfg(non_default_val=456)
    assert cfg.model_dump() == {'default_val': 123, 'non_default_val': 456}

    serialized_cli_args = CliApp.serialize(cfg)
    assert serialized_cli_args == ['--non_default_val', '456']

    assert CliApp.run(Cfg, cli_args=serialized_cli_args).model_dump() == cfg.model_dump()


def test_cli_serialize_ordering():
    class NestedCfg(BaseSettings):
        positional: CliPositionalArg[str]
        optional: int

    class Cfg(BaseSettings):
        command: CliSubCommand[NestedCfg]
        positional: CliPositionalArg[str]
        optional: int

    cfg = Cfg(optional=0, positional='pos_1', command=NestedCfg(optional=2, positional='pos_3'))
    assert cfg.model_dump() == {'command': {'optional': 2, 'positional': 'pos_3'}, 'optional': 0, 'positional': 'pos_1'}

    serialized_cli_args = CliApp.serialize(cfg)
    assert serialized_cli_args == [
        '--optional',
        '0',
        'pos_1',
        'command',
        '--optional',
        '2',
        'pos_3',
    ]

    assert CliApp.run(Cfg, cli_args=serialized_cli_args).model_dump() == cfg.model_dump()


def test_cli_decoding():
    PATH_A_STR = str(PureWindowsPath(Path.cwd()))
    PATH_B_STR = str(PureWindowsPath(Path.cwd() / 'subdir'))

    class PathsDecode(BaseSettings):
        path_a: Path = Field(validation_alias=AliasPath('paths', 0))
        path_b: Path = Field(validation_alias=AliasPath('paths', 1))
        num_a: int = Field(validation_alias=AliasPath('nums', 0))
        num_b: int = Field(validation_alias=AliasPath('nums', 1))

    assert CliApp.run(
        PathsDecode, cli_args=['--paths', PATH_A_STR, '--paths', PATH_B_STR, '--nums', '1', '--nums', '2']
    ).model_dump() == {
        'path_a': Path(PATH_A_STR),
        'path_b': Path(PATH_B_STR),
        'num_a': 1,
        'num_b': 2,
    }

    class PathsListNoDecode(BaseSettings):
        paths: Annotated[list[Path], NoDecode]
        nums: Annotated[list[int], NoDecode]

        @field_validator('paths', mode='before')
        @classmethod
        def decode_path_a(cls, paths: str) -> list[Path]:
            return [Path(p) for p in paths.split(',')]

        @field_validator('nums', mode='before')
        @classmethod
        def decode_nums(cls, nums: str) -> list[int]:
            return [int(n) for n in nums.split(',')]

    assert CliApp.run(
        PathsListNoDecode, cli_args=['--paths', f'{PATH_A_STR},{PATH_B_STR}', '--nums', '1,2']
    ).model_dump() == {'paths': [Path(PATH_A_STR), Path(PATH_B_STR)], 'nums': [1, 2]}

    class PathsAliasNoDecode(BaseSettings):
        path_a: Annotated[Path, NoDecode] = Field(validation_alias=AliasPath('paths', 0))
        path_b: Annotated[Path, NoDecode] = Field(validation_alias=AliasPath('paths', 1))
        num_a: Annotated[int, NoDecode] = Field(validation_alias=AliasPath('nums', 0))
        num_b: Annotated[int, NoDecode] = Field(validation_alias=AliasPath('nums', 1))

        @model_validator(mode='before')
        @classmethod
        def intercept_kwargs(cls, data: Any) -> Any:
            data['paths'] = [Path(p) for p in data['paths'].split(',')]
            data['nums'] = [int(n) for n in data['nums'].split(',')]
            return data

    assert CliApp.run(
        PathsAliasNoDecode, cli_args=['--paths', f'{PATH_A_STR},{PATH_B_STR}', '--nums', '1,2']
    ).model_dump() == {
        'path_a': Path(PATH_A_STR),
        'path_b': Path(PATH_B_STR),
        'num_a': 1,
        'num_b': 2,
    }

    with pytest.raises(
        SettingsError,
        match='Parsing error encountered for paths: Mixing Decode and NoDecode across different AliasPath fields is not allowed',
    ):

        class PathsMixedDecode(BaseSettings):
            path_a: Annotated[Path, ForceDecode] = Field(validation_alias=AliasPath('paths', 0))
            path_b: Annotated[Path, NoDecode] = Field(validation_alias=AliasPath('paths', 1))

        CliApp.run(PathsMixedDecode, cli_args=['--paths', PATH_A_STR, '--paths', PATH_B_STR])
