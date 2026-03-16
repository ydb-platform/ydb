import os

import requests
import hashlib
import logging
from cachetools import TTLCache
from sp_api.base import BaseClient

from .credentials import Credentials
from .access_token_response import AccessTokenResponse
from .exceptions import AuthorizationError

cache = TTLCache(maxsize=int(os.environ.get('SP_API_AUTH_CACHE_SIZE', 10)), ttl=3200)
grantless_cache = TTLCache(maxsize=int(os.environ.get('SP_API_AUTH_CACHE_SIZE', 10)), ttl=3200)

logger = logging.getLogger(__name__)


class AccessTokenClient(BaseClient):
    host = 'api.amazon.com'
    grant_type = 'refresh_token'
    path = '/auth/o2/token'

    def __init__(self, refresh_token=None, credentials=None, proxies=None, verify=True):
        self.cred = Credentials(refresh_token, credentials)
        self.proxies = proxies
        self.verify = verify

    def _request(self, url, data, headers):
        response = requests.post(url, data=data, headers=headers, proxies=self.proxies, verify=self.verify)
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
        try:
            access_token = cache[cache_key]
        except KeyError:
            request_url = self.scheme + self.host + self.path
            access_token = self._request(request_url, self.data, self.headers)
            cache[cache_key] = access_token
        return AccessTokenResponse(**access_token)

    def get_grantless_auth(self, scope='sellingpartnerapi::notifications'):
        """
        :param scope: One of allowed scope for grantless operations:
            sellingpartnerapi::notifications or sellingpartnerapi::migration
            See: https://github.com/amzn/selling-partner-api-docs/blob/main/guides/en-US/developer-guide/SellingPartnerApiDeveloperGuide.md#step-3-configure-your-lwa-credentials

        POST /auth/o2/token HTTP/l.l
        Host: api.amazon.com
        Content-Type: application/x-www-form-urlencoded;charset=UTF-8
        grant_type=client_credentials
        &scope=sellingpartnerapi::notifications
        &client_id=foodev
        &client_secret=Y76SDl2F
        :return: AccessTokenResponse
        """
        global grantless_cache
        cache_key = self._get_cache_key(scope)
        try:
            access_token = grantless_cache[cache_key]
            logger.debug('from_cache. scope: %s', scope)
        except KeyError:
            request_url = self.scheme + self.host + self.path
            access_token = self._request(
                request_url,
                data=self.grantless_data(scope),
                headers=self.headers
            )
            logger.debug('token_refreshed')
            grantless_cache.clear()
            grantless_cache[cache_key] = access_token

        return AccessTokenResponse(**access_token)

    def authorize_auth_code(self, auth_code):
        request_url = self.scheme + self.host + self.path
        res = self._request(
            request_url,
            data=self._auth_code_request_body(auth_code),
            headers=self.headers
        )
        return res

    def _auth_code_request_body(self, auth_code):
        return {
            'grant_type': 'authorization_code',
            'code': auth_code,
            'client_id': self.cred.client_id,
            'client_secret': self.cred.client_secret
        }

    def grantless_data(self, scope_value: str):
        return {
            'grant_type': 'client_credentials',
            'client_id': self.cred.client_id,
            'scope': scope_value,
            'client_secret': self.cred.client_secret
        }

    @property
    def data(self):
        return {
            'grant_type': self.grant_type,
            'client_id': self.cred.client_id,
            'refresh_token': self.cred.refresh_token,
            'client_secret': self.cred.client_secret
        }

    @property
    def headers(self):
        return {
            'User-Agent': self.user_agent,
            'content-type': self.content_type
        }

    def _get_cache_key(self, token_flavor=''):
        return 'access_token_' + hashlib.md5(
            (token_flavor + (self.cred.refresh_token or '__grantless__')).encode('utf-8')
        ).hexdigest()

