from __future__ import annotations as _annotations

from pathlib import Path

from pydantic import AnyHttpUrl, Field

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


def test_init_kwargs_override_env_for_alias_with_populate_by_name(env):
    class Settings(BaseSettings):
        abc: AnyHttpUrl = Field(validation_alias='my_abc')
        model_config = SettingsConfigDict(populate_by_name=True, extra='allow')

    env.set('MY_ABC', 'http://localhost.com')
    # Passing by field name should be accepted (populate_by_name=True) and should
    # override env-derived value. Also ensures init > env precedence with validation_alias.
    assert str(Settings(abc='http://prod.localhost.com/').abc) == 'http://prod.localhost.com/'


def test_precedence_init_over_env(tmp_path: Path, env):
    class Settings(BaseSettings):
        foo: str

    env.set('FOO', 'from-env')
    s = Settings(foo='from-init')
    assert s.foo == 'from-init'


def test_precedence_env_over_dotenv(tmp_path: Path, env):
    env_file = tmp_path / '.env'
    env_file.write_text('FOO=from-dotenv\n')

    class Settings(BaseSettings):
        foo: str

        model_config = SettingsConfigDict(env_file=env_file)

    env.set('FOO', 'from-env')
    s = Settings()
    assert s.foo == 'from-env'


def test_precedence_dotenv_over_secrets(tmp_path: Path):
    # create dotenv
    env_file = tmp_path / '.env'
    env_file.write_text('FOO=from-dotenv\n')

    # create secrets directory with same key
    secrets_dir = tmp_path / 'secrets'
    secrets_dir.mkdir()
    (secrets_dir / 'FOO').write_text('from-secrets\n')

    class Settings(BaseSettings):
        foo: str

        model_config = SettingsConfigDict(env_file=env_file, secrets_dir=secrets_dir)

    # No env set, dotenv should override secrets
    s = Settings()
    assert s.foo == 'from-dotenv'


def test_precedence_secrets_over_defaults(tmp_path: Path):
    secrets_dir = tmp_path / 'secrets'
    secrets_dir.mkdir()
    (secrets_dir / 'FOO').write_text('from-secrets\n')

    class Settings(BaseSettings):
        foo: str = 'from-default'

        model_config = SettingsConfigDict(secrets_dir=secrets_dir)

    s = Settings()
    assert s.foo == 'from-secrets'


def test_merging_preserves_earlier_values(tmp_path: Path, env):
    # Prove that merging preserves earlier source values: init -> env -> dotenv -> secrets -> defaults
    # We'll populate nested from dotenv and env parts, then set a default for a, and init for b
    env_file = tmp_path / '.env'
    env_file.write_text('NESTED={"x":1}\n')

    secrets_dir = tmp_path / 'secrets'
    secrets_dir.mkdir()
    (secrets_dir / 'NESTED').write_text('{"y": 2}')

    class Settings(BaseSettings):
        a: int = 10
        b: int = 0
        nested: dict

        model_config = SettingsConfigDict(env_file=env_file, secrets_dir=secrets_dir, env_nested_delimiter='__')

        @classmethod
        def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
        ):
            # normal order; we want to assert deep merging
            return init_settings, env_settings, dotenv_settings, file_secret_settings

    # env contributes nested.y and overrides dotenv nested.x=1 if set; we'll set only y to prove merge
    env.set('NESTED__y', '3')
    # init contributes b, defaults contribute a
    s = Settings(b=20)
    assert s.a == 10  # defaults preserved
    assert s.b == 20  # init wins
    # nested: dotenv provides x=1; env provides y=3; deep merged => {x:1, y:3}
    assert s.nested == {'x': 1, 'y': 3}
