"""
Test pydantic_settings.AWSSecretsManagerSettingsSource.
"""

import json
import os

import pytest

try:
    import yaml
    from moto import mock_aws
except ImportError:
    yaml = None
    mock_aws = None

from pydantic import BaseModel, Field

from pydantic_settings import (
    AWSSecretsManagerSettingsSource,
    BaseSettings,
    PydanticBaseSettingsSource,
)
from pydantic_settings.sources.providers.aws import import_aws_secrets_manager

try:
    aws_secrets_manager = True
    import_aws_secrets_manager()
    import boto3

    os.environ['AWS_DEFAULT_REGION'] = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
except ImportError:
    aws_secrets_manager = False


MODULE = 'pydantic_settings.sources'

if not yaml:
    pytest.skip('PyYAML is not installed', allow_module_level=True)


@pytest.mark.skipif(not aws_secrets_manager, reason='pydantic-settings[aws-secrets-manager] is not installed')
class TestAWSSecretsManagerSettingsSource:
    """Test AWSSecretsManagerSettingsSource."""

    @mock_aws
    def test_repr(self) -> None:
        client = boto3.client('secretsmanager')
        client.create_secret(Name='test-secret', SecretString='{}')

        source = AWSSecretsManagerSettingsSource(BaseSettings, 'test-secret')
        assert repr(source) == "AWSSecretsManagerSettingsSource(secret_id='test-secret', env_nested_delimiter='--')"

    @mock_aws
    def test___init__(self) -> None:
        """Test __init__."""

        class AWSSecretsManagerSettings(BaseSettings):
            """AWSSecretsManager settings."""

        client = boto3.client('secretsmanager')
        client.create_secret(Name='test-secret', SecretString='{}')

        AWSSecretsManagerSettingsSource(AWSSecretsManagerSettings, 'test-secret')

    @mock_aws
    def test___call__(self) -> None:
        """Test __call__."""

        class SqlServer(BaseModel):
            password: str = Field(..., alias='Password')

        class AWSSecretsManagerSettings(BaseSettings):
            """AWSSecretsManager settings."""

            sql_server_user: str = Field(..., alias='SqlServerUser')
            sql_server: SqlServer = Field(..., alias='SqlServer')

        secret_data = {'SqlServerUser': 'test-user', 'SqlServer--Password': 'test-password'}

        client = boto3.client('secretsmanager')
        client.create_secret(Name='test-secret', SecretString=json.dumps(secret_data))

        obj = AWSSecretsManagerSettingsSource(AWSSecretsManagerSettings, 'test-secret')

        settings = obj()

        assert settings['SqlServerUser'] == 'test-user'
        assert settings['SqlServer']['Password'] == 'test-password'

    @mock_aws
    def test_secret_manager_case_insensitive_success(self) -> None:
        """Test secret manager getitem case insensitive success."""

        class SqlServer(BaseModel):
            password: str = Field(..., alias='Password')

        class AWSSecretsManagerSettings(BaseSettings):
            """AWSSecretsManager settings."""

            sql_server_user: str
            sql_server: SqlServer

            @classmethod
            def settings_customise_sources(
                cls,
                settings_cls: type[BaseSettings],
                init_settings: PydanticBaseSettingsSource,
                env_settings: PydanticBaseSettingsSource,
                dotenv_settings: PydanticBaseSettingsSource,
                file_secret_settings: PydanticBaseSettingsSource,
            ) -> tuple[PydanticBaseSettingsSource, ...]:
                return (AWSSecretsManagerSettingsSource(settings_cls, 'test-secret', case_sensitive=False),)

        secret_data = {
            'SQL_SERVER_USER': 'test-user',
            'SQL_SERVER--PASSWORD': 'test-password',
        }

        client = boto3.client('secretsmanager')
        client.create_secret(Name='test-secret', SecretString=json.dumps(secret_data))

        settings = AWSSecretsManagerSettings()  # type: ignore

        assert settings.sql_server_user == 'test-user'
        assert settings.sql_server.password == 'test-password'

    @mock_aws
    def test_aws_secrets_manager_settings_source(self) -> None:
        """Test AWSSecretsManagerSettingsSource."""

        class SqlServer(BaseModel):
            password: str = Field(..., alias='Password')

        class AWSSecretsManagerSettings(BaseSettings):
            """AWSSecretsManager settings."""

            sql_server_user: str = Field(..., alias='SqlServerUser')
            sql_server: SqlServer = Field(..., alias='SqlServer')

            @classmethod
            def settings_customise_sources(
                cls,
                settings_cls: type[BaseSettings],
                init_settings: PydanticBaseSettingsSource,
                env_settings: PydanticBaseSettingsSource,
                dotenv_settings: PydanticBaseSettingsSource,
                file_secret_settings: PydanticBaseSettingsSource,
            ) -> tuple[PydanticBaseSettingsSource, ...]:
                return (AWSSecretsManagerSettingsSource(settings_cls, 'test-secret'),)

        secret_data = {'SqlServerUser': 'test-user', 'SqlServer--Password': 'test-password'}

        client = boto3.client('secretsmanager')
        client.create_secret(Name='test-secret', SecretString=json.dumps(secret_data))

        settings = AWSSecretsManagerSettings()  # type: ignore

        assert settings.sql_server_user == 'test-user'
        assert settings.sql_server.password == 'test-password'
