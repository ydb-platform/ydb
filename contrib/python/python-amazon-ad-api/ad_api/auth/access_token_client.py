import requests
import hashlib
import logging
from cachetools import TTLCache
from ad_api.base import BaseClient

from .credentials import Credentials
from .access_token_response import AccessTokenResponse
from .exceptions import AuthorizationError

# from .credential_provider import CredentialProvider

import os

cache = TTLCache(maxsize=int(os.environ.get('AD_API_AUTH_CACHE_SIZE', 10)), ttl=3200)
grantless_cache = TTLCache(maxsize=int(os.environ.get('AD_API_AUTH_CACHE_SIZE', 10)), ttl=3200)

logger = logging.getLogger(__name__)


class AccessTokenClient(BaseClient):
    host = 'api.amazon.com'
    grant_type = 'refresh_token'
    path = '/auth/o2/token'

    def __init__(self, credentials=None, proxies=None, verify=True, timeout=None):
        self.cred = Credentials(credentials)
        self.timeout = timeout
        self.proxies = proxies
        self.verify = verify

    def _request(self, url, data, headers):
        response = requests.post(
            url,
            data=data,
            headers=headers,
            timeout=self.timeout,
            proxies=self.proxies,
            verify=self.verify,
        )
        response_data = response.json()
        if response.status_code != 200:
            error_message = response_data.get('error_description')
            error_code = response_data.get('error')
            raise AuthorizationError(error_code, error_message, response.status_code)
        return response_data

    def get_auth(self) -> AccessTokenResponse:
        """
        Get's the access token
        :return:AccessTokenResponse
        """

        cache_key = self._get_cache_key()
        access_token = cache.get(cache_key)
        if access_token is None:
            request_url = self.scheme + self.host + self.path
            access_token = self._request(request_url, self.data, self.headers)
            cache[cache_key] = access_token
        return AccessTokenResponse(**access_token)

    def authorize_auth_code(self, auth_code):
        request_url = self.scheme + self.host + self.path
        res = self._request(request_url, data=self._auth_code_request_body(auth_code), headers=self.headers)
        return res

    def _auth_code_request_body(self, auth_code):
        return {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'client_id': self.cred.client_id,
            'client_secret': self.cred.client_secret,
        }

    @property
    def data(self):
        return {
            'grant_type': self.grant_type,
            'client_id': self.cred.client_id,
            'refresh_token': self.cred.refresh_token,
            'client_secret': self.cred.client_secret,
        }

    @property
    def headers(self):
        return {'User-Agent': self.user_agent, 'content-type': self.content_type}

    def _get_cache_key(self, token_flavor=''):
        return 'access_token_' + hashlib.md5((token_flavor + self.cred.refresh_token).encode('utf-8')).hexdigest()
