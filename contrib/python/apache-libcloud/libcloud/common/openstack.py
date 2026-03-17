# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Common utilities for OpenStack
"""

from libcloud.utils.py3 import ET, httplib
from libcloud.common.base import Response, ConnectionUserAndKey
from libcloud.common.types import ProviderError
from libcloud.compute.types import LibcloudError, MalformedResponseError, KeyPairDoesNotExistError
from libcloud.common.exceptions import BaseHTTPError

# Imports for backward compatibility reasons
from libcloud.common.openstack_identity import (
    AUTH_TOKEN_HEADER,
    OpenStackServiceCatalog,
    OpenStackIdentityTokenScope,
    get_class_for_auth_version,
)

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore

AUTH_API_VERSION = "1.1"

# Auth versions which contain token expiration information.
AUTH_VERSIONS_WITH_EXPIRES = [
    "1.1",
    "2.0",
    "2.0_apikey",
    "2.0_password",
    "3.x",
    "3.x_password",
]

__all__ = [
    "OpenStackBaseConnection",
    "OpenStackResponse",
    "OpenStackException",
    "OpenStackDriverMixin",
]


class OpenStackBaseConnection(ConnectionUserAndKey):
    """
    Base class for OpenStack connections.

    :param user_id: User name to use when authenticating
    :type user_id: ``str``

    :param key: Secret to use when authenticating.
    :type key: ``str``

    :param secure: Use HTTPS?  (True by default.)
    :type secure: ``bool``

    :param ex_force_base_url: Base URL for connection requests.  If
                              not specified, this will be determined by
                              authenticating.
    :type ex_force_base_url: ``str``

    :param ex_force_auth_url: Base URL for authentication requests.
    :type ex_force_auth_url: ``str``

    :param ex_force_auth_version: Authentication version to use.  If
                                  not specified, defaults to AUTH_API_VERSION.
    :type ex_force_auth_version: ``str``

    :param ex_force_auth_token: Authentication token to use for connection
                                requests.  If specified, the connection will
                                not attempt to authenticate, and the value
                                of ex_force_base_url will be used to
                                determine the base request URL.  If
                                ex_force_auth_token is passed in,
                                ex_force_base_url must also be provided.
    :type ex_force_auth_token: ``str``

    :param token_scope: Whether to scope a token to a "project", a
                        "domain" or "unscoped".
    :type token_scope: ``str``

    :param ex_domain_name: When authenticating, provide this domain name to
                           the identity service.  A scoped token will be
                           returned. Some cloud providers require the domain
                           name to be provided at authentication time. Others
                           will use a default domain if none is provided.
    :type ex_domain_name: ``str``

    :param ex_tenant_name: When authenticating, provide this tenant name to the
                           identity service. A scoped token will be returned.
                           Some cloud providers require the tenant name to be
                           provided at authentication time. Others will use a
                           default tenant if none is provided.
    :type ex_tenant_name: ``str``

    :param ex_tenant_domain_id: When authenticating, provide this tenant
                                domain id to the identity service.
                                A scoped token will be returned.
                                Some cloud providers require the tenant
                                domain id to be provided at authentication
                                time. Others will use a default tenant
                                domain id if none is provided.
    :type ex_tenant_domain_id: ``str``

    :param ex_force_service_type: Service type to use when selecting an
                                  service. If not specified, a provider
                                  specific default will be used.
    :type ex_force_service_type: ``str``

    :param ex_force_service_name: Service name to use when selecting an
                                  service. If not specified, a provider
                                  specific default will be used.
    :type ex_force_service_name: ``str``

    :param ex_force_service_region: Region to use when selecting an service.
                                    If not specified, a provider specific
                                    default will be used.
    :type ex_force_service_region: ``str``

    :param ex_auth_cache: External cache where authentication tokens are
                          stored for reuse by other processes. Tokens are
                          always cached in memory on the driver instance. To
                          share tokens among multiple drivers, processes, or
                          systems, pass a cache here.
    :type ex_auth_cache: :class:`OpenStackAuthenticationCache`
    """

    auth_url = None  # type: str
    auth_token = None  # type: str
    auth_token_expires = None
    auth_user_info = None
    service_catalog = None
    service_type = None
    service_name = None
    service_region = None
    accept_format = None
    _auth_version = None  # type: str

    def __init__(
        self,
        user_id,
        key,
        secure=True,
        host=None,
        port=None,
        timeout=None,
        proxy_url=None,
        ex_force_base_url=None,
        ex_force_auth_url=None,
        ex_force_auth_version=None,
        ex_force_auth_token=None,
        ex_token_scope=OpenStackIdentityTokenScope.PROJECT,
        ex_domain_name="Default",
        ex_tenant_name=None,
        ex_tenant_domain_id="default",
        ex_force_service_type=None,
        ex_force_service_name=None,
        ex_force_service_region=None,
        ex_force_microversion=None,
        ex_auth_cache=None,
        retry_delay=None,
        backoff=None,
    ):
        super().__init__(
            user_id,
            key,
            secure=secure,
            timeout=timeout,
            retry_delay=retry_delay,
            backoff=backoff,
            proxy_url=proxy_url,
        )

        if ex_force_auth_version:
            self._auth_version = ex_force_auth_version

        self.base_url = ex_force_base_url
        self._ex_force_base_url = ex_force_base_url
        self._ex_force_auth_url = ex_force_auth_url
        self._ex_force_auth_token = ex_force_auth_token
        self._ex_token_scope = ex_token_scope
        self._ex_domain_name = ex_domain_name
        self._ex_tenant_name = ex_tenant_name
        self._ex_tenant_domain_id = ex_tenant_domain_id
        self._ex_force_service_type = ex_force_service_type
        self._ex_force_service_name = ex_force_service_name
        self._ex_force_service_region = ex_force_service_region
        self._ex_force_microversion = ex_force_microversion
        self._ex_auth_cache = ex_auth_cache
        self._osa = None

        if ex_force_auth_token and not ex_force_base_url:
            raise LibcloudError(
                "Must also provide ex_force_base_url when specifying " "ex_force_auth_token."
            )

        if ex_force_auth_token:
            self.auth_token = ex_force_auth_token

        if not self._auth_version:
            self._auth_version = AUTH_API_VERSION

        auth_url = self._get_auth_url()

        if not auth_url:
            raise LibcloudError("OpenStack instance must " + "have auth_url set")

    def get_auth_class(self):
        """
        Retrieve identity / authentication class instance.

        :rtype: :class:`OpenStackIdentityConnection`
        """
        if not self._osa:
            auth_url = self._get_auth_url()

            cls = get_class_for_auth_version(auth_version=self._auth_version)
            self._osa = cls(
                auth_url=auth_url,
                user_id=self.user_id,
                key=self.key,
                tenant_name=self._ex_tenant_name,
                tenant_domain_id=self._ex_tenant_domain_id,
                domain_name=self._ex_domain_name,
                token_scope=self._ex_token_scope,
                timeout=self.timeout,
                proxy_url=self.proxy_url,
                parent_conn=self,
                auth_cache=self._ex_auth_cache,
            )

        return self._osa

    def request(self, action, params=None, data="", headers=None, method="GET", raw=False):
        headers = headers or {}
        params = params or {}

        # Include default content-type for POST and PUT request (if available)
        default_content_type = getattr(self, "default_content_type", None)
        if method.upper() in ["POST", "PUT"] and default_content_type:
            headers["Content-Type"] = default_content_type

        try:
            return super().request(
                action=action,
                params=params,
                data=data,
                method=method,
                headers=headers,
                raw=raw,
            )
        except BaseHTTPError as ex:
            # Evict cached auth token if we receive Unauthorized while using it
            if ex.code == httplib.UNAUTHORIZED and self._ex_force_auth_token is None:
                self.get_auth_class().clear_cached_auth_context()
            raise

    def _get_auth_url(self):
        """
        Retrieve auth url for this instance using either "ex_force_auth_url"
        constructor kwarg of "auth_url" class variable.
        """
        auth_url = self.auth_url

        if self._ex_force_auth_url is not None:
            auth_url = self._ex_force_auth_url

        return auth_url

    def get_service_catalog(self):
        if self.service_catalog is None:
            self._populate_hosts_and_request_paths()

        return self.service_catalog

    def get_service_name(self):
        """
        Gets the service name used to look up the endpoint in the service
        catalog.

        :return: name of the service in the catalog
        """
        if self._ex_force_service_name:
            return self._ex_force_service_name

        return self.service_name

    def get_endpoint(self):
        """
        Selects the endpoint to use based on provider specific values,
        or overrides passed in by the user when setting up the driver.

        :returns: url of the relevant endpoint for the driver
        """
        service_type = self.service_type
        service_name = self.service_name
        service_region = self.service_region

        if self._ex_force_service_type:
            service_type = self._ex_force_service_type
        if self._ex_force_service_name:
            service_name = self._ex_force_service_name
        if self._ex_force_service_region:
            service_region = self._ex_force_service_region

        endpoint = self.service_catalog.get_endpoint(
            service_type=service_type, name=service_name, region=service_region
        )

        url = endpoint.url

        if not url:
            raise LibcloudError("Could not find specified endpoint")

        return url

    def add_default_headers(self, headers):
        headers[AUTH_TOKEN_HEADER] = self.auth_token
        headers["Accept"] = self.accept_format
        if self._ex_force_microversion:
            # If service not set in microversion, assume compute
            microversion = self._ex_force_microversion.strip().split()
            if len(microversion) == 2:
                service_type = microversion[0]
                microversion = microversion[1]
            elif len(microversion) == 1:
                service_type = "compute"
                microversion = microversion[0]
            else:
                raise LibcloudError("Invalid microversion format: servicename X.XX")

            if self.service_type and self.service_type.startswith(service_type):
                headers["OpenStack-API-Version"] = "{} {}".format(
                    service_type,
                    microversion,
                )
        return headers

    def morph_action_hook(self, action):
        self._populate_hosts_and_request_paths()
        return super().morph_action_hook(action)

    def _set_up_connection_info(self, url):
        prev_conn = (self.host, self.port, self.secure)
        result = self._tuple_from_url(url)
        (self.host, self.port, self.secure, self.request_path) = result
        new_conn = (self.host, self.port, self.secure)
        if new_conn != prev_conn:
            # We only call connect in case connection details have changed - this way we correctly
            # reuse connection in case nothing has changed
            self.connect()

    def _populate_hosts_and_request_paths(self):
        """
        OpenStack uses a separate host for API calls which is only provided
        after an initial authentication request.
        """
        osa = self.get_auth_class()

        if self._ex_force_auth_token:
            # If ex_force_auth_token is provided we always hit the api directly
            # and never try to authenticate.
            #
            # Note: When ex_force_auth_token is provided, ex_force_base_url
            # must be provided as well.
            self._set_up_connection_info(url=self._ex_force_base_url)
            return

        if not osa.is_token_valid():
            # Token is not available or it has expired. Need to retrieve a
            # new one.
            if self._auth_version == "2.0_apikey":
                kwargs = {"auth_type": "api_key"}
            elif self._auth_version == "2.0_password":
                kwargs = {"auth_type": "password"}
            else:
                kwargs = {}

            # pylint: disable=unexpected-keyword-arg
            osa = osa.authenticate(**kwargs)  # may throw InvalidCreds
            # pylint: enable=unexpected-keyword-arg

            self.auth_token = osa.auth_token
            self.auth_token_expires = osa.auth_token_expires
            self.auth_user_info = osa.auth_user_info

            # Pull out and parse the service catalog
            osc = OpenStackServiceCatalog(service_catalog=osa.urls, auth_version=self._auth_version)
            self.service_catalog = osc

        url = self._ex_force_base_url or self.get_endpoint()
        self._set_up_connection_info(url=url)


class OpenStackException(ProviderError):
    pass


class OpenStackResponse(Response):
    node_driver = None

    def success(self):
        i = int(self.status)
        return 200 <= i <= 299

    def has_content_type(self, content_type):
        content_type_value = self.headers.get("content-type") or ""
        content_type_value = content_type_value.lower()
        return content_type_value.find(content_type.lower()) > -1

    def parse_body(self):
        if self.status == httplib.NO_CONTENT or not self.body:
            return None

        if self.has_content_type("application/xml"):
            try:
                return ET.XML(self.body)
            except Exception:
                raise MalformedResponseError(
                    "Failed to parse XML", body=self.body, driver=self.node_driver
                )

        elif self.has_content_type("application/json"):
            try:
                return json.loads(self.body)
            except Exception:
                raise MalformedResponseError(
                    "Failed to parse JSON", body=self.body, driver=self.node_driver
                )
        else:
            return self.body

    def parse_error(self):
        body = self.parse_body()

        if self.has_content_type("application/xml"):
            text = "; ".join([err.text or "" for err in body.getiterator() if err.text])
        elif self.has_content_type("application/json"):
            values = list(body.values())

            context = self.connection.context
            driver = self.connection.driver
            key_pair_name = context.get("key_pair_name", None)

            if (
                len(values) > 0
                and "code" in values[0]
                and values[0]["code"] == 404
                and key_pair_name
            ):
                raise KeyPairDoesNotExistError(name=key_pair_name, driver=driver)
            elif len(values) > 0 and "message" in values[0]:
                text = ";".join([fault_data["message"] for fault_data in values])
            else:
                text = body
        else:
            # while we hope a response is always one of xml or json, we have
            # seen html or text in the past, its not clear we can really do
            # something to make it more readable here, so we will just pass
            # it along as the whole response body in the text variable.
            text = body

        return "{} {} {}".format(self.status, self.error, text)


class OpenStackDriverMixin:
    def __init__(
        self,
        ex_force_base_url=None,
        ex_force_auth_url=None,
        ex_force_auth_version=None,
        ex_force_auth_token=None,
        ex_token_scope=OpenStackIdentityTokenScope.PROJECT,
        ex_domain_name="Default",
        ex_tenant_name=None,
        ex_tenant_domain_id="default",
        ex_force_service_type=None,
        ex_force_service_name=None,
        ex_force_service_region=None,
        ex_auth_cache=None,
        ex_force_microversion=None,
        *args,
        **kwargs,
    ):
        self._ex_force_base_url = ex_force_base_url
        self._ex_force_auth_url = ex_force_auth_url
        self._ex_force_auth_version = ex_force_auth_version
        self._ex_force_auth_token = ex_force_auth_token
        self._ex_token_scope = ex_token_scope
        self._ex_domain_name = ex_domain_name
        self._ex_tenant_name = ex_tenant_name
        self._ex_tenant_domain_id = ex_tenant_domain_id
        self._ex_force_service_type = ex_force_service_type
        self._ex_force_service_name = ex_force_service_name
        self._ex_force_service_region = ex_force_service_region
        self._ex_auth_cache = ex_auth_cache
        self._ex_force_microversion = ex_force_microversion

    def openstack_connection_kwargs(self):
        """
        Returns certain ``ex_*`` parameters for this connection.

        :rtype: ``dict``
        """
        rv = {}
        if self._ex_force_base_url:
            rv["ex_force_base_url"] = self._ex_force_base_url
        if self._ex_force_auth_token:
            rv["ex_force_auth_token"] = self._ex_force_auth_token
        if self._ex_force_auth_url:
            rv["ex_force_auth_url"] = self._ex_force_auth_url
        if self._ex_force_auth_version:
            rv["ex_force_auth_version"] = self._ex_force_auth_version
        if self._ex_token_scope:
            rv["ex_token_scope"] = self._ex_token_scope
        if self._ex_domain_name:
            rv["ex_domain_name"] = self._ex_domain_name
        if self._ex_tenant_name:
            rv["ex_tenant_name"] = self._ex_tenant_name
        if self._ex_tenant_domain_id:
            rv["ex_tenant_domain_id"] = self._ex_tenant_domain_id
        if self._ex_force_service_type:
            rv["ex_force_service_type"] = self._ex_force_service_type
        if self._ex_force_service_name:
            rv["ex_force_service_name"] = self._ex_force_service_name
        if self._ex_force_service_region:
            rv["ex_force_service_region"] = self._ex_force_service_region
        if self._ex_auth_cache is not None:
            rv["ex_auth_cache"] = self._ex_auth_cache
        if self._ex_force_microversion:
            rv["ex_force_microversion"] = self._ex_force_microversion
        return rv
