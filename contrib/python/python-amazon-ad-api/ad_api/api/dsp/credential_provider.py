from ad_api.base.config import BaseConfig
from ad_api.base.credential_provider import CredentialProvider


class DspCredentialProvider(CredentialProvider):
    config_class = BaseConfig

    def from_env(self):
        account_data = dict(
            refresh_token=self._get_env('AD_API_REFRESH_TOKEN'),
            client_id=self._get_env('AD_API_CLIENT_ID'),
            client_secret=self._get_env('AD_API_CLIENT_SECRET'),
        )
        self.credentials = self.config_class(**account_data)
        return len(self.credentials.check_config()) == 0
