"""
Test pydantic_settings.AzureKeyVaultSettingsSource.
"""

import pytest
from pydantic import BaseModel, Field
from pytest_mock import MockerFixture

from pydantic_settings import (
    AzureKeyVaultSettingsSource,
    BaseSettings,
    PydanticBaseSettingsSource,
)
from pydantic_settings.sources.providers.azure import import_azure_key_vault

try:
    azure_key_vault = True
    import_azure_key_vault()
    from azure.core.exceptions import ResourceNotFoundError
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import KeyVaultSecret, SecretClient, SecretProperties
except ImportError:
    azure_key_vault = False


@pytest.mark.skipif(not azure_key_vault, reason='pydantic-settings[azure-key-vault] is not installed')
class TestAzureKeyVaultSettingsSource:
    """Test AzureKeyVaultSettingsSource."""

    def test___init__(self, mocker: MockerFixture) -> None:
        """Test __init__."""

        class AzureKeyVaultSettings(BaseSettings):
            """AzureKeyVault settings."""

        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.list_properties_of_secrets.__qualname__}',
            return_value=[],
        )

        AzureKeyVaultSettingsSource(
            AzureKeyVaultSettings, 'https://my-resource.vault.azure.net/', DefaultAzureCredential()
        )

    def test___call__(self, mocker: MockerFixture) -> None:
        """Test __call__."""

        class SqlServer(BaseModel):
            password: str = Field(..., alias='Password')

        class AzureKeyVaultSettings(BaseSettings):
            """AzureKeyVault settings."""

            SqlServerUser: str
            sql_server_user: str = Field(..., alias='SqlServerUser')
            sql_server: SqlServer = Field(..., alias='SqlServer')

        expected_secrets = [
            type('', (), {'name': 'SqlServerUser', 'enabled': True}),
            type('', (), {'name': 'SqlServer--Password', 'enabled': True}),
        ]
        expected_secret_value = 'SecretValue'
        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.list_properties_of_secrets.__qualname__}',
            return_value=expected_secrets,
        )
        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.get_secret.__qualname__}',
            side_effect=self._raise_resource_not_found_when_getting_parent_secret_name,
        )
        obj = AzureKeyVaultSettingsSource(
            AzureKeyVaultSettings, 'https://my-resource.vault.azure.net/', DefaultAzureCredential()
        )

        settings = obj()

        assert settings['SqlServerUser'] == expected_secret_value
        assert settings['SqlServer']['Password'] == expected_secret_value

    def test_do_not_load_disabled_secrets(self, mocker: MockerFixture) -> None:
        class AzureKeyVaultSettings(BaseSettings):
            """AzureKeyVault settings."""

            SqlServerPassword: str
            DisabledSqlServerPassword: str

        disabled_secret_name = 'SqlServerPassword'
        expected_secrets = [
            type('', (), {'name': disabled_secret_name, 'enabled': False}),
        ]
        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.list_properties_of_secrets.__qualname__}',
            return_value=expected_secrets,
        )
        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.get_secret.__qualname__}',
            return_value=KeyVaultSecret(SecretProperties(), 'SecretValue'),
        )
        obj = AzureKeyVaultSettingsSource(
            AzureKeyVaultSettings, 'https://my-resource.vault.azure.net/', DefaultAzureCredential()
        )

        settings = obj()

        assert disabled_secret_name not in settings

    def test_azure_key_vault_settings_source(self, mocker: MockerFixture) -> None:
        """Test AzureKeyVaultSettingsSource."""

        class SqlServer(BaseModel):
            password: str = Field(..., alias='Password')

        class AzureKeyVaultSettings(BaseSettings):
            """AzureKeyVault settings."""

            SqlServerUser: str
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
                return (
                    AzureKeyVaultSettingsSource(
                        settings_cls, 'https://my-resource.vault.azure.net/', DefaultAzureCredential()
                    ),
                )

        expected_secrets = [
            type('', (), {'name': 'SqlServerUser', 'enabled': True}),
            type('', (), {'name': 'SqlServer--Password', 'enabled': True}),
        ]
        expected_secret_value = 'SecretValue'
        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.list_properties_of_secrets.__qualname__}',
            return_value=expected_secrets,
        )
        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.get_secret.__qualname__}',
            side_effect=self._raise_resource_not_found_when_getting_parent_secret_name,
        )

        settings = AzureKeyVaultSettings()  # type: ignore

        assert settings.SqlServerUser == expected_secret_value
        assert settings.sql_server_user == expected_secret_value
        assert settings.sql_server.password == expected_secret_value

    def _raise_resource_not_found_when_getting_parent_secret_name(self, secret_name: str):
        expected_secret_value = 'SecretValue'
        key_vault_secret = KeyVaultSecret(SecretProperties(), expected_secret_value)

        if secret_name == 'SqlServer':
            raise ResourceNotFoundError()

        return key_vault_secret

    def test_dash_to_underscore_translation(self, mocker: MockerFixture) -> None:
        """Test that dashes in secret names are mapped to underscores in field names."""

        class AzureKeyVaultSettings(BaseSettings):
            my_field: str
            alias_field: str = Field(..., alias='Secret-Alias')

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
                    AzureKeyVaultSettingsSource(
                        settings_cls,
                        'https://my-resource.vault.azure.net/',
                        DefaultAzureCredential(),
                        dash_to_underscore=True,
                    ),
                )

        expected_secrets = [
            type('', (), {'name': 'my-field', 'enabled': True}),
            type('', (), {'name': 'Secret-Alias', 'enabled': True}),
        ]
        expected_secret_value = 'SecretValue'

        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.list_properties_of_secrets.__qualname__}',
            return_value=expected_secrets,
        )
        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.get_secret.__qualname__}',
            return_value=KeyVaultSecret(SecretProperties(), expected_secret_value),
        )

        settings = AzureKeyVaultSettings()

        assert settings.my_field == expected_secret_value
        assert settings.alias_field == expected_secret_value

    def test_snake_case_conversion(self, mocker: MockerFixture) -> None:
        """Test that secret names are mapped to snake case in field names."""

        class NestedModel(BaseModel):
            nested_field: str

        class AzureKeyVaultSettings(BaseSettings):
            my_field_from_kebab_case: str
            my_field_from_pascal_case: str
            my_field_from_camel_case: str
            alias_field: str = Field(alias='Secret-Alias')
            alias_field_2: str = Field(alias='another-SECRET-AliaS')
            nested_model: NestedModel

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
                    AzureKeyVaultSettingsSource(
                        settings_cls,
                        'https://my-resource.vault.azure.net/',
                        DefaultAzureCredential(),
                        snake_case_conversion=True,
                    ),
                )

        expected_secrets = [
            type('', (), {'name': 'my-field-from-kebab-case', 'enabled': True}),
            type('', (), {'name': 'MyFieldFromPascalCase', 'enabled': True}),
            type('', (), {'name': 'myFieldFromCamelCase', 'enabled': True}),
            type('', (), {'name': 'Secret-Alias', 'enabled': True}),
            type('', (), {'name': 'another-SECRET-AliaS', 'enabled': True}),
            type('', (), {'name': 'NestedModel--NestedField', 'enabled': True}),
        ]
        expected_secret_value = 'SecretValue'

        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.list_properties_of_secrets.__qualname__}',
            return_value=expected_secrets,
        )
        mocker.patch(
            f'{AzureKeyVaultSettingsSource.__module__}.{SecretClient.get_secret.__qualname__}',
            return_value=KeyVaultSecret(SecretProperties(), expected_secret_value),
        )

        settings = AzureKeyVaultSettings()

        assert settings.my_field_from_kebab_case == expected_secret_value
        assert settings.my_field_from_pascal_case == expected_secret_value
        assert settings.my_field_from_camel_case == expected_secret_value
        assert settings.alias_field == expected_secret_value
        assert settings.alias_field_2 == expected_secret_value
        assert settings.nested_model.nested_field == expected_secret_value
