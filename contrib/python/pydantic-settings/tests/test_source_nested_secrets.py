from enum import Enum
from os import sep

import pytest
from pydantic import BaseModel

from pydantic_settings import (
    BaseSettings,
    NestedSecretsSettingsSource,
    SecretsSettingsSource,
    SettingsConfigDict,
    SettingsError,
)
from pydantic_settings.sources.providers.nested_secrets import SECRETS_DIR_MAX_SIZE


class DbSettings(BaseModel):
    user: str
    passwd: str | None = None


class AppSettings(BaseSettings):
    app_key: str | None = None
    db: DbSettings

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls,
        init_settings,
        env_settings,
        dotenv_settings,
        file_secret_settings,
    ):
        return (
            init_settings,
            env_settings,
            NestedSecretsSettingsSource(file_secret_settings),
        )


class SampleEnum(str, Enum):
    TEST = 'test'


def test_repr(tmp_path):
    class Settings(BaseSettings):
        model_config = SettingsConfigDict(
            secrets_dir=tmp_path,
        )

    src = NestedSecretsSettingsSource(SecretsSettingsSource(Settings))
    assert f'{src!r}'.startswith(f'{src.__class__.__name__}(')


def test_source_off(env, tmp_files):
    env.set('DB__USER', 'user')
    tmp_files.write(
        {
            'app_key': 'secret1',
            'db__passwd': 'secret2',
        }
    )

    class Settings(AppSettings):
        model_config = SettingsConfigDict(
            env_nested_delimiter='__',
        )

    assert Settings().model_dump() == {
        'app_key': None,
        'db': {'user': 'user', 'passwd': None},
    }


def test_delimited_name(env, tmp_files):
    env.set('DB__USER', 'user')
    tmp_files.write(
        {
            'app_key': 'secret1',
            'db___passwd': 'secret2',
        }
    )

    class Settings(AppSettings):
        model_config = SettingsConfigDict(
            env_nested_delimiter='__',
            secrets_dir=tmp_files.basedir,
            secrets_nested_delimiter='___',
        )

    assert Settings().model_dump() == {
        'app_key': 'secret1',
        'db': {'user': 'user', 'passwd': 'secret2'},
    }


def test_secrets_dir_as_arg(env, tmp_files):
    env.set('DB__USER', 'user')
    tmp_files.write(
        {
            'app_key': 'secret1',
            'db__passwd': 'secret2',
        }
    )

    class Settings(AppSettings):
        model_config = SettingsConfigDict(
            env_nested_delimiter='__',
            secrets_nested_delimiter='__',
        )

    assert Settings(_secrets_dir=tmp_files.basedir).model_dump() == {
        'app_key': 'secret1',
        'db': {'user': 'user', 'passwd': 'secret2'},
    }


@pytest.mark.parametrize(
    'conf,secrets',
    (
        (
            dict(secrets_nested_delimiter='___', secrets_prefix='prefix_'),
            {'prefix_app_key': 'secret1', 'prefix_db___passwd': 'secret2'},
        ),
        (
            dict(secrets_nested_subdir=True, secrets_prefix='prefix_'),
            {'prefix_app_key': 'secret1', 'prefix_db/passwd': 'secret2'},
        ),
        (
            dict(secrets_nested_subdir=True, secrets_prefix=f'dir1{sep}dir2{sep}'),
            {'dir1/dir2/app_key': 'secret1', 'dir1/dir2/db/passwd': 'secret2'},
        ),
    ),
)
def test_prefix(conf: SettingsConfigDict, secrets, env, tmp_files):
    env.set('DB__USER', 'user')
    tmp_files.write(secrets)

    class Settings(AppSettings):
        model_config = SettingsConfigDict(
            env_nested_delimiter='__',
            secrets_dir=tmp_files.basedir,
            **conf,
        )

    assert Settings().model_dump() == {
        'app_key': 'secret1',
        'db': {'user': 'user', 'passwd': 'secret2'},
    }


def test_subdir(env, tmp_files):
    env.set('DB__USER', 'user')
    tmp_files.write(
        {
            'app_key': 'secret1',
            'db/passwd': 'secret2',  # file in subdir
        }
    )

    class Settings(AppSettings):
        model_config = SettingsConfigDict(
            secrets_dir=tmp_files.basedir,
            env_nested_delimiter='__',
            secrets_nested_subdir=True,
        )

    assert Settings().model_dump() == {
        'app_key': 'secret1',
        'db': {'user': 'user', 'passwd': 'secret2'},
    }


def test_symlink_subdir(env, tmp_files):
    env.set('DB__USER', 'user')
    # fmt: off
    tmp_files.write({
        'app_key': 'secret1',
        'db_random/passwd': 'secret2',  # file in subdir that is not directly referenced in our settings
    })
    tmp_files.basedir.joinpath('db').symlink_to(tmp_files.basedir.joinpath('db_random'))
    # fmt: on

    class Settings(AppSettings):
        model_config = SettingsConfigDict(
            secrets_dir=tmp_files.basedir,
            env_nested_delimiter='__',
            secrets_nested_subdir=True,
        )

    assert Settings().model_dump() == {
        'app_key': 'secret1',
        'db': {'user': 'user', 'passwd': 'secret2'},
    }


@pytest.mark.parametrize(
    'conf,secrets,dirs,expected',
    (
        (
            # when multiple secrets_dir values are given, their values are merged
            dict(),
            {'dir1/key1': 'a', 'dir1/key2': 'b', 'dir2/key2': 'c'},
            ['dir1', 'dir2'],
            {'key1': 'a', 'key2': 'c'},
        ),
        (
            # when secrets_dir is not a directory, error is raised
            dict(),
            {'some_file': ''},
            'some_file',
            (SettingsError, 'must reference a directory'),
        ),
        (
            # missing secrets_dir emits warning by default
            dict(),
            {'key1': 'value'},
            'missing_subdir',
            (UserWarning, 1, 'does not exist', {'key1': None, 'key2': None}),
        ),
        (
            # ...or expect warning explicitly (identical behaviour)
            dict(secrets_dir_missing='warn'),
            {'key1': 'value'},
            'missing_subdir',
            (UserWarning, 1, 'does not exist', {'key1': None, 'key2': None}),
        ),
        (
            # missing secrets_dir warning can be suppressed
            dict(secrets_dir_missing='ok'),
            {'key1': 'value'},
            'missing_subdir',
            {'key1': None, 'key2': None},
        ),
        (
            # missing secrets_dir can raise error
            dict(secrets_dir_missing='error'),
            {'key1': 'value'},
            'missing_subdir',
            (SettingsError, 'does not exist'),
        ),
        (
            # invalid secrets_dir_missing value raises error
            dict(secrets_dir_missing='uNeXpEcTeD'),
            {'key1': 'value'},
            'missing_subdir',
            (SettingsError, 'invalid secrets_dir_missing value'),
        ),
        (
            # when multiple secrets_dir do not exist, multiple warnings are emitted
            dict(),
            {'key1': 'value'},
            ['missing_subdir1', 'missing_subdir2'],
            (UserWarning, 2, 'does not exist', {'key1': None, 'key2': None}),
        ),
        (
            # secrets_dir size is limited
            dict(),
            {'key1': 'x' * SECRETS_DIR_MAX_SIZE},
            '.',
            {'key1': 'x' * SECRETS_DIR_MAX_SIZE, 'key2': None},
        ),
        (
            # ...and raises error if file is larger than the limit
            dict(),
            {'key1': 'x' * (SECRETS_DIR_MAX_SIZE + 1)},
            '.',
            (SettingsError, 'secrets_dir size'),
        ),
        (
            # secrets_dir size limit can be adjusted
            dict(secrets_dir_max_size=100),
            {'key1': 'x' * 100},
            '.',
            {'key1': 'x' * 100, 'key2': None},
        ),
        (
            # ...and raises error if file is larger than the limit
            dict(secrets_dir_max_size=100),
            {'key1': 'x' * 101},
            '.',
            (SettingsError, 'secrets_dir size'),
        ),
        (
            # ...even if secrets_dir size exceeds limit because of another file
            dict(secrets_dir_max_size=100),
            {'another_file': 'x' * 101},
            '.',
            (SettingsError, 'secrets_dir size'),
        ),
        (
            # when multiple secrets_dir values are given, their sizes are not added
            dict(secrets_dir_max_size=100),
            {'dir1/key1': 'x' * 100, 'dir2/key2': 'y' * 100},
            ['dir1', 'dir2'],
            {'key1': 'x' * 100, 'key2': 'y' * 100},
        ),
    ),
)
def test_multiple_secrets_dirs(conf: SettingsConfigDict, secrets, dirs, expected, tmp_files):
    secrets_dirs = [tmp_files.basedir / d for d in dirs] if isinstance(dirs, list) else tmp_files.basedir / dirs
    tmp_files.write(secrets)

    class Settings(BaseSettings):
        key1: str | None = None
        key2: str | None = None

        model_config = SettingsConfigDict(secrets_dir=secrets_dirs, **conf)

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls,
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        ):
            return (NestedSecretsSettingsSource(file_secret_settings),)

    # clean execution
    if isinstance(expected, dict):
        assert Settings().model_dump() == expected
    # error
    elif isinstance(expected, tuple) and len(expected) == 2:
        error_type, msg_fragment = expected
        with pytest.raises(error_type, match=msg_fragment):
            Settings()
    # warnings
    elif isinstance(expected, tuple) and len(expected) == 4:
        warning_type, warning_count, msg_fragment, value = expected
        with pytest.warns(warning_type) as warninfo:
            settings = Settings()
        assert len(warninfo) == warning_count
        assert all(msg_fragment in str(w.message) for w in warninfo)
        assert settings.model_dump() == value
    # unexpected
    else:
        raise AssertionError('unreachable')


def test_strip_whitespace(env, tmp_files):
    env.set('DB__USER', 'user')
    tmp_files.write(
        {
            'app_key': ' secret1 ',
            'db__passwd': '\tsecret2\n',
        }
    )

    class Settings(AppSettings):
        model_config = SettingsConfigDict(
            env_nested_delimiter='__',
            secrets_dir=tmp_files.basedir,
            secrets_nested_delimiter='__',
        )

    assert Settings(_secrets_dir=tmp_files.basedir).model_dump() == {
        'app_key': 'secret1',
        'db': {'user': 'user', 'passwd': 'secret2'},
    }


def test_invalid_options(tmp_path):
    class Settings(AppSettings):
        model_config = SettingsConfigDict(
            secrets_dir=tmp_path,
            env_nested_delimiter='__',
            secrets_nested_subdir=True,
            secrets_nested_delimiter='__',
        )

    with pytest.raises(SettingsError, match='mutually exclusive'):
        Settings()


@pytest.mark.parametrize(
    'conf,expected',
    (
        # default settings
        ({}, dict(field_empty='', field_none='null', field_enum=SampleEnum.TEST)),
        # env_ignore_empty has no effect on secrets
        ({'env_ignore_empty': True}, dict(field_empty='')),
        ({'env_ignore_empty': False}, dict(field_empty='')),
        # env_parse_none_str has no effect on secrets
        ({'env_parse_none_str': 'null'}, dict(field_none='null')),
        # env_parse_enums has no effect on secrets
        ({'env_parse_enums': True}, dict(field_enum=SampleEnum.TEST)),
        ({'env_parse_enums': False}, dict(field_enum=SampleEnum.TEST)),
    ),
)
def test_env_ignore_empty(conf: SettingsConfigDict, expected, tmp_files):
    tmp_files.write(
        {
            'field_empty': '',
            'field_none': 'null',
            'field_enum': 'test',
        }
    )

    class Settings(BaseSettings):
        field_empty: str | None = None
        field_none: str | None = None
        field_enum: SampleEnum | None = None

    class Original(Settings):
        model_config = SettingsConfigDict(secrets_dir=tmp_files.basedir, **conf)

    class Evaluated(Settings):
        model_config = SettingsConfigDict(secrets_dir=tmp_files.basedir, **conf)

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls,
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
        ):
            return (NestedSecretsSettingsSource(file_secret_settings),)

    original = Original()
    evaluated = Evaluated()
    assert original.model_dump() == evaluated.model_dump()
    for k, v in expected.items():
        assert getattr(original, k) == getattr(evaluated, k) == v
