from ad_api.api.dsp.credential_provider import DspCredentialProvider
from ad_api.auth.access_token_client import AccessTokenClient


class DspAccessTokenClient(AccessTokenClient):
    credential_provider_class = DspCredentialProvider
