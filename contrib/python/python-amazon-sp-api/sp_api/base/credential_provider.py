import abc
import functools
import json
import os

from typing import Dict, Iterable, Optional, Type

import confuse
import boto3
from botocore.exceptions import ClientError
from cachetools import Cache

try:
    from aws_secretsmanager_caching import SecretCache
except ImportError:
    SecretCache = None


required_credentials = [
    'lwa_app_id',
    'lwa_client_secret'
]


class MissingCredentials(Exception):
    """
    Credentials are missing, see the error output to find possible causes
    """
    pass


class BaseCredentialProvider(abc.ABC):
    errors = []
    credentials = None

    def __init__(self, account: str = 'default', *args, **kwargs):
        self.account = account

    def __call__(self, *args, **kwargs):
        self.load_credentials()
        return self.check_credentials()

    @abc.abstractmethod
    def load_credentials(self):
        ...

    def check_credentials(self):
        try:
            self.errors = [c for c in required_credentials if
                           c not in self.credentials.keys() or not self.credentials[c]]
        except (AttributeError, TypeError):
            raise MissingCredentials(f'Credentials are missing: {", ".join(required_credentials)}')
        if not len(self.errors):
            return self.credentials
        raise MissingCredentials(f'Credentials are missing: {", ".join(self.errors)}')


class FromCodeCredentialProvider(BaseCredentialProvider):
    def load_credentials(self):
        return None

    def __init__(self, credentials: dict, *args, **kwargs):
        super(FromCodeCredentialProvider, self).__init__('default', credentials)
        self.credentials = credentials


class FromConfigFileCredentialProvider(BaseCredentialProvider):
    def load_credentials(self):
        try:
            config = confuse.Configuration('python-sp-api')
            config_filename = os.path.join(config.config_dir(), 'credentials.yml')
            config.set_file(config_filename)
            account_data = config[self.account].get()
            self.credentials = account_data
        except (confuse.exceptions.NotFoundError, confuse.exceptions.ConfigReadError):
            return


class BaseFromSecretsCredentialProvider(BaseCredentialProvider):
    def load_credentials(self):
        secret_id = os.environ.get('SP_API_AWS_SECRET_ID')
        if not secret_id:
            return
        secret = self.get_secret_content(secret_id)
        if not secret:
            return
        self.credentials = dict(
            refresh_token=secret.get('SP_API_REFRESH_TOKEN'),
            lwa_app_id=secret.get('LWA_APP_ID'),
            lwa_client_secret=secret.get('LWA_CLIENT_SECRET'),
            aws_secret_key=secret.get('SP_API_SECRET_KEY'),
            aws_access_key=secret.get('SP_API_ACCESS_KEY'),
            role_arn=secret.get('SP_API_ROLE_ARN')
        )

    @abc.abstractmethod
    def get_secret_content(self, secret_id: str) -> Dict[str, str]:
        ...


class FromSecretsCredentialProvider(BaseFromSecretsCredentialProvider):
    def get_secret_content(self, secret_id: str) -> Dict[str, str]:
        try:
            client = boto3.client('secretsmanager')
            response = client.get_secret_value(SecretId=secret_id)
            return json.loads(response.get('SecretString'))
        except ClientError:
            return {}


class FromCachedSecretsCredentialProvider(BaseFromSecretsCredentialProvider):
    def get_secret_content(self, secret_id: str) -> Dict[str, str]:
        # If caching library is not available, return.
        secret_cache = self._get_secret_cache()
        if not secret_cache:
            return {}
        try:
            response = secret_cache.get_secret_string(secret_id=secret_id)
            return json.loads(response)
        except ClientError:
            return {}

    @classmethod
    @functools.lru_cache(maxsize=None)
    def _get_secret_cache(cls):
        if SecretCache is None:
            return None
        return SecretCache()


class FromEnvironmentVariablesCredentialProvider(BaseCredentialProvider):
    def load_credentials(self):
        account_data = dict(
            refresh_token=self._get_env('SP_API_REFRESH_TOKEN'),
            lwa_app_id=self._get_env('LWA_APP_ID'),
            lwa_client_secret=self._get_env('LWA_CLIENT_SECRET'),
            aws_secret_key=self._get_env('SP_API_SECRET_KEY'),
            aws_access_key=self._get_env('SP_API_ACCESS_KEY'),
            role_arn=self._get_env('SP_API_ROLE_ARN')
        )
        self.credentials = account_data

    def _get_env(self, key):
        return os.environ.get(f'{key}_{self.account}',
                              os.environ.get(key))


class CredentialProvider:
    credentials = None
    cache = Cache(maxsize=10)

    CREDENTIAL_PROVIDERS: Iterable[Type[BaseCredentialProvider]] = (
        FromCodeCredentialProvider,
        FromEnvironmentVariablesCredentialProvider,
        FromCachedSecretsCredentialProvider,
        FromSecretsCredentialProvider,
        FromConfigFileCredentialProvider
    )

    def __init__(
        self,
        account: str = 'default',
        credentials: Optional[Dict[str, str]] = None,
        credential_providers: Optional[Iterable[Type[BaseCredentialProvider]]] = None,
    ):
        self.account = account
        providers = self.CREDENTIAL_PROVIDERS if credential_providers is None else credential_providers
        for cp in providers:
            try:
                self.credentials = cp(account=account, credentials=credentials)()
                break
            except MissingCredentials:
                continue
        if self.credentials:
            self.credentials = self.Config(**self.credentials)
        else:
            raise MissingCredentials(f'Credentials are missing: {", ".join(required_credentials)}')

    class Config:
        def __init__(self, **kwargs):
            self.refresh_token = kwargs.get('refresh_token')
            self.lwa_app_id = kwargs.get('lwa_app_id')
            self.lwa_client_secret = kwargs.get('lwa_client_secret')
            self.aws_access_key = kwargs.get('aws_access_key')
            self.aws_secret_key = kwargs.get('aws_secret_key')
            self.role_arn = kwargs.get('role_arn')
