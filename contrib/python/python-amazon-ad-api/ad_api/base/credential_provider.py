import abc
import os

from typing import Any, Dict, Optional

import confuse
import logging

logger = logging.getLogger(__name__)
REQUIRED_CREDENTIALS = [
    'refresh_token',
    'client_id',
    'client_secret',
]
ADDITIONAL_CREDENTIALS = [
    'profile_id'
]


class MissingCredentials(Exception):
    """
    Credentials are missing, see the error output to find possible causes
    """

    pass


class BaseCredentialProvider(abc.ABC):
    def __init__(self, credentials: Dict[str, str], *args: Any, **kwargs: Any):
        self.credentials = credentials

    @abc.abstractmethod
    def load_credentials(self, verify_additional_credentials: bool) -> Dict[str, str]:
        pass

    def _get_env(self, key: str) -> str:
        return os.environ.get(key)

    def check_credentials(self, verify_additional_credentials: bool) -> Dict[str, str]:
        creds_to_check = REQUIRED_CREDENTIALS[:]
        if verify_additional_credentials:
            creds_to_check += ADDITIONAL_CREDENTIALS
        try:
            self.errors = [c for c in creds_to_check if c not in self.credentials.keys() or not self.credentials[c]]
        except (AttributeError, TypeError):
            raise MissingCredentials(f'Credentials are missing: {", ".join(creds_to_check)}')
        if not len(self.errors):
            return self.credentials
        raise MissingCredentials(f'Credentials are missing: {", ".join(self.errors)}')


class FromEnvCredentialProvider(BaseCredentialProvider):
    def __init__(self, *args: Any, **kwargs: Any):
        pass

    def load_credentials(self, verify_additional_credentials: bool) -> Dict[str, str]:
        account_data = dict(
            refresh_token=self._get_env('AD_API_REFRESH_TOKEN'),
            client_id=self._get_env('AD_API_CLIENT_ID'),
            client_secret=self._get_env('AD_API_CLIENT_SECRET'),
            profile_id=self._get_env('AD_API_PROFILE_ID'),
        )
        self.credentials = account_data
        self.check_credentials(verify_additional_credentials)
        return self.credentials


class FromCodeCredentialProvider(BaseCredentialProvider):
    def __init__(self, credentials: Dict, *args: Any, **kwargs: Any):
        super().__init__(credentials)

    def load_credentials(self, verify_additional_credentials: bool) -> Dict[str, str]:
        self.check_credentials(verify_additional_credentials)
        return self.credentials


class FromConfigFileCredentialProvider(BaseCredentialProvider):
    file = 'credentials.yml'  # Will moved to default confuse config.yaml

    def __init__(self, account: str, *args: Any, **kwargs: Any):
        self.account = account

    def load_credentials(self, verify_additional_credentials: bool) -> Dict[str, str]:
        try:
            config = confuse.Configuration('python-ad-api')
            config_filename = os.path.join(config.config_dir(), self.file)
            config.set_file(config_filename)
            account_data = config[self.account].get()
            super().__init__(account_data)
            self.check_credentials(verify_additional_credentials)
            return account_data

        except confuse.exceptions.NotFoundError:
            raise MissingCredentials(f'The account {self.account} was not setup in your configuration file.')
        except confuse.exceptions.ConfigReadError:
            raise MissingCredentials(
                f'Neither environment variables nor a config file were found. '
                f'Please set the correct variables, or use a config file (credentials.yml). '
                f'See https://confuse.readthedocs.io/en/latest/usage.html#search-paths for search paths.'
            )


class CredentialProvider:
    def load_credentials(self, verify_additional_credentials: bool) -> Dict[str, str]:
        return {}

    def _get_env(self, key: str) -> Optional[str]:
        return os.environ.get(f'{key}', os.environ.get(key))

    def __init__(
        self,
        account: str = 'default',
        credentials: Optional[Dict[str, str]] = None,
        verify_additional_credentials: bool = True,
    ):
        client_id_env = self._get_env('AD_API_CLIENT_ID')
        client_secret_env = self._get_env('AD_API_CLIENT_SECRET')
        refresh_token_env = self._get_env('AD_API_REFRESH_TOKEN')

        if client_id_env is not None and client_secret_env is not None and refresh_token_env is not None:
            try:
                self.credentials = FromEnvCredentialProvider().load_credentials(verify_additional_credentials)
            except MissingCredentials:
                logging.error("MissingCredentials")
                logging.error(MissingCredentials)

        elif isinstance(credentials, dict):
            try:
                self.credentials = FromCodeCredentialProvider(credentials).load_credentials(verify_additional_credentials)

            except MissingCredentials:
                logging.error("MissingCredentials")
                logging.error(MissingCredentials)

        else:
            try:
                self.credentials = FromConfigFileCredentialProvider(account).load_credentials(verify_additional_credentials)

            except MissingCredentials:
                logging.error("MissingCredentials")
                logging.error(MissingCredentials)

    class Config:
        def __init__(self, **kwargs: Any):
            self.refresh_token = kwargs.get('refresh_token')
            self.client_id = kwargs.get('client_id')
            self.client_secret = kwargs.get('client_secret')
            self.profile_id = kwargs.get('profile_id')
