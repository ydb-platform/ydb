import hashlib
import json
from datetime import datetime
import logging
import os
from json import JSONDecodeError

from cachetools import TTLCache
from requests import request

from sp_api.auth import AccessTokenClient, AccessTokenResponse
from .ApiResponse import ApiResponse
from .base_client import BaseClient
from .exceptions import get_exception_for_code, MissingScopeException
from .marketplaces import Marketplaces
from sp_api.base.credential_provider import CredentialProvider

log = logging.getLogger(__name__)

role_cache = TTLCache(maxsize=int(os.environ.get('SP_API_AUTH_CACHE_SIZE', 10)), ttl=3200)


class Client(BaseClient):
    grantless_scope: str = ''
    keep_restricted_data_token: bool = False
    version = None

    def __init__(
            self,
            marketplace: Marketplaces = Marketplaces[
                os.environ.get('SP_API_DEFAULT_MARKETPLACE', Marketplaces.US.name)],
            *,
            refresh_token=None,
            account='default',
            credentials=None,
            restricted_data_token=None,
            proxies=None,
            verify=True,
            timeout=None,
            version=None,
            credential_providers=None,
    ):
        if os.environ.get('SP_API_DEFAULT_MARKETPLACE', None):
            marketplace = Marketplaces[os.environ.get('SP_API_DEFAULT_MARKETPLACE')]
        self.credentials = CredentialProvider(
            account,
            credentials,
            credential_providers=credential_providers,
        ).credentials

        self.endpoint = marketplace.endpoint
        self.marketplace_id = marketplace.marketplace_id
        self.region = marketplace.region
        self.restricted_data_token = restricted_data_token
        self._auth = AccessTokenClient(refresh_token=refresh_token, credentials=self.credentials, proxies=proxies, verify=verify)
        self.proxies = proxies
        self.timeout = timeout
        self.version = version
        self.verify = verify

    def _get_cache_key(self, token_flavor=''):
        return 'role_' + hashlib.md5(
            (token_flavor + self._auth.cred.refresh_token).encode('utf-8')
        ).hexdigest()

    @property
    def headers(self):
        return {
            'host': self.endpoint[8:],
            'user-agent': self.user_agent,
            'x-amz-access-token': self.restricted_data_token or self.auth.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'content-type': 'application/json'
        }

    @property
    def auth(self) -> AccessTokenResponse:
        return self._auth.get_auth()

    @property
    def grantless_auth(self) -> AccessTokenResponse:
        if not self.grantless_scope:
            raise MissingScopeException("Grantless operations require scope")
        return self._auth.get_grantless_auth(self.grantless_scope)

    def _request(self, path: str, *, data: dict = None, params: dict = None, headers=None,
                 add_marketplace=True, res_no_data: bool = False, bulk: bool = False,
                 wrap_list: bool = False) -> ApiResponse:
        if params is None:
            params = {}
        if data is None:
            data = {}

        # Note: The use of isinstance here is to support request schemas that are an array at the
        # top level, eg get_product_fees_estimate
        self.method = params.pop('method', data.pop('method', 'GET') if isinstance(data, dict) else 'GET')

        if add_marketplace:
            self._add_marketplaces(data if self.method in ('POST', 'PUT') else params)

        res = request(self.method,
                      self.endpoint + self._check_version(path),
                      params=params,
                      data=json.dumps(data) if data and self.method in ('POST', 'PUT', 'PATCH') else None,
                      headers=headers or self.headers,
                      timeout=self.timeout,
                      proxies=self.proxies,
                      verify=self.verify)
        return self._check_response(res, res_no_data, bulk, wrap_list)

    def _check_response(self, res, res_no_data: bool = False, bulk: bool = False,
                        wrap_list: bool = False) -> ApiResponse:
        if (self.method == 'DELETE' or res_no_data) and 200 <= res.status_code < 300:
            try:
                js = res.json() or {}
            except JSONDecodeError:
                js = {'status_code': res.status_code}
        else:
            try:
                js = res.json() or {}
            except JSONDecodeError:
                js = {}

        if isinstance(js, list):
            if wrap_list:
                # Support responses that are an array at the top level, eg get_product_fees_estimate
                js = dict(payload=js)
            else:
                js = js[0]

        error = js.get('errors', None)

        if error:
            exception = get_exception_for_code(res.status_code)
            raise exception(error, headers=res.headers)
        return ApiResponse(**js, headers=res.headers)

    def _add_marketplaces(self, data):
        POST = ['marketplaceIds', 'MarketplaceIds']
        GET = ['MarketplaceId', 'MarketplaceIds', 'marketplace_ids', 'marketplaceIds']

        if self.method == 'POST':
            if any(x in data.keys() for x in POST):
                return
            return data.update({k: self.marketplace_id if not k.endswith('s') else [self.marketplace_id] for k in POST})
        if any(x in data.keys() for x in GET):
            return
        return data.update({k: self.marketplace_id if not k.endswith('s') else [self.marketplace_id] for k in GET})

    def _request_grantless_operation(self, path: str, *, data: dict = None, params: dict = None):
        headers = {
            'host': self.endpoint[8:],
            'user-agent': self.user_agent,
            'x-amz-access-token': self.grantless_auth.access_token,
            'x-amz-date': datetime.utcnow().strftime('%Y%m%dT%H%M%SZ'),
            'content-type': 'application/json'
        }

        return self._request(path, data=data, params=params, headers=headers)

    def _check_version(self, path):
        if '<version>' not in path:
            return path
        return path.replace('<version>', self.version)

    def __enter__(self):
        self.keep_restricted_data_token = True
        return self

    def __exit__(self, *args, **kwargs):
        self.restricted_data_token = None
        self.keep_restricted_data_token = False
