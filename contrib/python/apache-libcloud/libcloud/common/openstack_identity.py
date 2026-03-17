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
Common / shared code for handling authentication against OpenStack identity
service (Keystone).
"""

import datetime
from collections import namedtuple

from libcloud.utils.py3 import httplib
from libcloud.common.base import Response, ConnectionUserAndKey, CertificateConnection
from libcloud.compute.types import LibcloudError, InvalidCredsError, MalformedResponseError
from libcloud.utils.iso8601 import parse_date

try:
    import simplejson as json
except ImportError:
    import json  # type: ignore

AUTH_API_VERSION = "1.1"
AUTH_TOKEN_HEADER = "X-Auth-Token"

# Auth versions which contain token expiration information.
AUTH_VERSIONS_WITH_EXPIRES = [
    "1.1",
    "2.0",
    "2.0_apikey",
    "2.0_password",
    "2.0_voms",
    "3.0",
    "3.x_password",
    "3.x_appcred",
    "3.x_oidc_access_token",
]

# How many seconds to subtract from the auth token expiration time before
# testing if the token is still valid.
# The time is subtracted to account for the HTTP request latency and prevent
# user from getting "InvalidCredsError" if token is about to expire.
AUTH_TOKEN_EXPIRES_GRACE_SECONDS = 5


__all__ = [
    "OpenStackAuthenticationCache",
    "OpenStackAuthenticationCacheKey",
    "OpenStackAuthenticationContext",
    "OpenStackIdentityVersion",
    "OpenStackIdentityDomain",
    "OpenStackIdentityProject",
    "OpenStackIdentityUser",
    "OpenStackIdentityRole",
    "OpenStackServiceCatalog",
    "OpenStackServiceCatalogEntry",
    "OpenStackServiceCatalogEntryEndpoint",
    "OpenStackIdentityEndpointType",
    "OpenStackIdentityConnection",
    "OpenStackIdentity_1_0_Connection",
    "OpenStackIdentity_1_1_Connection",
    "OpenStackIdentity_2_0_Connection",
    "OpenStackIdentity_2_0_Connection_VOMS",
    "OpenStackIdentity_3_0_Connection",
    "OpenStackIdentity_3_0_Connection_AppCred",
    "OpenStackIdentity_3_0_Connection_OIDC_access_token",
    "get_class_for_auth_version",
]


class OpenStackAuthenticationCache:
    """
    Base class for external OpenStack authentication caches.

    Authentication tokens are always cached in memory in
    :class:`OpenStackIdentityConnection`.auth_token and related fields.  These
    tokens are lost when the driver is garbage collected.  To share tokens
    among multiple drivers, processes, or systems, use an
    :class:`OpenStackAuthenticationCache` in
    OpenStackIdentityConnection.auth_cache.

    Cache implementers should inherit this class and define the methods below.
    """

    def get(self, key):
        """
        Get an authentication context from the cache.

        :param key: Key to fetch.
        :type key: :class:`.OpenStackAuthenticationCacheKey`

        :return: The cached context for the given key, if present; None if not.
        :rtype: :class:`OpenStackAuthenticationContext`
        """
        raise NotImplementedError

    def put(self, key, context):
        """
        Put an authentication context into the cache.

        :param key: Key where the context will be stored.
        :type key: :class:`.OpenStackAuthenticationCacheKey`

        :param context: The context to cache.
        :type context: :class:`.OpenStackAuthenticationContext`
        """
        raise NotImplementedError

    def clear(self, key):
        """
        Clear an authentication context from the cache.

        :param key: Key to clear.
        :type key: :class:`.OpenStackAuthenticationCacheKey`
        """
        raise NotImplementedError


OpenStackAuthenticationCacheKey = namedtuple(
    "OpenStackAuthenticationCacheKey",
    [
        "auth_url",
        "user_id",
        "token_scope",
        "tenant_name",
        "domain_name",
        "tenant_domain_id",
    ],
)


class OpenStackAuthenticationContext:
    """
    An authentication token and related context.
    """

    def __init__(self, token, expiration=None, user=None, roles=None, urls=None):
        self.token = token
        self.expiration = expiration
        self.user = user
        self.roles = roles
        self.urls = urls


class OpenStackIdentityEndpointType:
    """
    Enum class for openstack identity endpoint type.
    """

    INTERNAL = "internal"
    EXTERNAL = "external"
    ADMIN = "admin"


class OpenStackIdentityTokenScope:
    """
    Enum class for openstack identity token scope.
    """

    PROJECT = "project"
    DOMAIN = "domain"
    UNSCOPED = "unscoped"


class OpenStackIdentityVersion:
    def __init__(self, version, status, updated, url):
        self.version = version
        self.status = status
        self.updated = updated
        self.url = url

    def __repr__(self):
        return "<OpenStackIdentityVersion version=%s, status=%s, " "updated=%s, url=%s>" % (
            self.version,
            self.status,
            self.updated,
            self.url,
        )


class OpenStackIdentityDomain:
    def __init__(self, id, name, enabled):
        self.id = id
        self.name = name
        self.enabled = enabled

    def __repr__(self):
        return "<OpenStackIdentityDomain id={}, name={}, enabled={}>".format(
            self.id,
            self.name,
            self.enabled,
        )


class OpenStackIdentityProject:
    def __init__(self, id, name, description, enabled, domain_id=None):
        self.id = id
        self.name = name
        self.description = description
        self.enabled = enabled
        self.domain_id = domain_id

    def __repr__(self):
        return "<OpenStackIdentityProject id=%s, domain_id=%s, name=%s, " "enabled=%s>" % (
            self.id,
            self.domain_id,
            self.name,
            self.enabled,
        )


class OpenStackIdentityRole:
    def __init__(self, id, name, description, enabled):
        self.id = id
        self.name = name
        self.description = description
        self.enabled = enabled

    def __repr__(self):
        return "<OpenStackIdentityRole id=%s, name=%s, description=%s, " "enabled=%s>" % (
            self.id,
            self.name,
            self.description,
            self.enabled,
        )


class OpenStackIdentityUser:
    def __init__(self, id, domain_id, name, email, description, enabled):
        self.id = id
        self.domain_id = domain_id
        self.name = name
        self.email = email
        self.description = description
        self.enabled = enabled

    def __repr__(self):
        return "<OpenStackIdentityUser id=%s, domain_id=%s, name=%s, " "email=%s, enabled=%s>" % (
            self.id,
            self.domain_id,
            self.name,
            self.email,
            self.enabled,
        )


class OpenStackServiceCatalog:
    """
    http://docs.openstack.org/api/openstack-identity-service/2.0/content/

    This class should be instantiated with the contents of the
    'serviceCatalog' in the auth response. This will do the work of figuring
    out which services actually exist in the catalog as well as split them up
    by type, name, and region if available
    """

    _auth_version = None
    _service_catalog = None

    def __init__(self, service_catalog, auth_version=AUTH_API_VERSION):
        self._auth_version = auth_version

        # Check this way because there are a couple of different 2.0_*
        # auth types.
        if "3.x" in self._auth_version:
            entries = self._parse_service_catalog_auth_v3(service_catalog=service_catalog)
        elif "2.0" in self._auth_version:
            entries = self._parse_service_catalog_auth_v2(service_catalog=service_catalog)
        elif ("1.1" in self._auth_version) or ("1.0" in self._auth_version):
            entries = self._parse_service_catalog_auth_v1(service_catalog=service_catalog)
        else:
            raise LibcloudError('auth version "%s" not supported' % (self._auth_version))

        # Force consistent ordering by sorting the entries
        entries = sorted(entries, key=lambda x: x.service_type + (x.service_name or ""))
        self._entries = entries  # stories all the service catalog entries

    def get_entries(self):
        """
        Return all the entries for this service catalog.

        :rtype: ``list`` of :class:`.OpenStackServiceCatalogEntry`
        """
        return self._entries

    def get_catalog(self):
        """
        Deprecated in the favor of ``get_entries`` method.
        """
        return self.get_entries()

    def get_public_urls(self, service_type=None, name=None):
        """
        Retrieve all the available public (external) URLs for the provided
        service type and name.
        """
        endpoints = self.get_endpoints(service_type=service_type, name=name)

        result = []
        for endpoint in endpoints:
            endpoint_type = endpoint.endpoint_type
            if endpoint_type == OpenStackIdentityEndpointType.EXTERNAL:
                result.append(endpoint.url)

        return result

    def get_endpoints(self, service_type=None, name=None):
        """
        Retrieve all the endpoints for the provided service type and name.

        :rtype: ``list`` of :class:`.OpenStackServiceCatalogEntryEndpoint`
        """
        endpoints = []

        for entry in self._entries:
            # Note: "if XXX and YYY != XXX" comparison is used to support
            # partial lookups.
            # This allows user to pass in only one argument to the method (only
            # service_type or name), both of them or neither.
            if service_type and entry.service_type != service_type:
                continue

            if name and entry.service_name != name:
                continue

            for endpoint in entry.endpoints:
                endpoints.append(endpoint)

        return endpoints

    def get_endpoint(
        self,
        service_type=None,
        name=None,
        region=None,
        endpoint_type=OpenStackIdentityEndpointType.EXTERNAL,
    ):
        """
        Retrieve a single endpoint using the provided criteria.

        Note: If no or more than one matching endpoint is found, an exception
        is thrown.
        """
        endpoints = []

        for entry in self._entries:
            if service_type and entry.service_type != service_type:
                continue

            if name and entry.service_name != name:
                continue

            for endpoint in entry.endpoints:
                if region and endpoint.region != region:
                    continue

                if endpoint_type and endpoint.endpoint_type != endpoint_type:
                    continue

                endpoints.append(endpoint)

        if len(endpoints) == 1:
            return endpoints[0]
        elif len(endpoints) > 1:
            raise ValueError("Found more than 1 matching endpoint")
        else:
            raise LibcloudError("Could not find specified endpoint")

    def get_regions(self, service_type=None):
        """
        Retrieve a list of all the available regions.

        :param service_type: If specified, only return regions for this
                             service type.
        :type service_type: ``str``

        :rtype: ``list`` of ``str``
        """
        regions = set()

        for entry in self._entries:
            if service_type and entry.service_type != service_type:
                continue

            for endpoint in entry.endpoints:
                if endpoint.region:
                    regions.add(endpoint.region)

        return sorted(list(regions))

    def get_service_types(self, region=None):
        """
        Retrieve all the available service types.

        :param region: Optional region to retrieve service types for.
        :type region: ``str``

        :rtype: ``list`` of ``str``
        """
        service_types = set()

        for entry in self._entries:
            include = True

            for endpoint in entry.endpoints:
                if region and endpoint.region != region:
                    include = False
                    break

            if include:
                service_types.add(entry.service_type)

        return sorted(list(service_types))

    def get_service_names(self, service_type=None, region=None):
        """
        Retrieve list of service names that match service type and region.

        :type service_type: ``str``
        :type region: ``str``

        :rtype: ``list`` of ``str``
        """
        names = set()

        if "2.0" not in self._auth_version:
            raise ValueError("Unsupported version: %s" % (self._auth_version))

        for entry in self._entries:
            if service_type and entry.service_type != service_type:
                continue

            include = True
            for endpoint in entry.endpoints:
                if region and endpoint.region != region:
                    include = False
                    break

            if include and entry.service_name:
                names.add(entry.service_name)

        return sorted(list(names))

    def _parse_service_catalog_auth_v1(self, service_catalog):
        entries = []

        for service, endpoints in service_catalog.items():
            entry_endpoints = []
            for endpoint in endpoints:
                region = endpoint.get("region", None)

                public_url = endpoint.get("publicURL", None)
                private_url = endpoint.get("internalURL", None)

                if public_url:
                    entry_endpoint = OpenStackServiceCatalogEntryEndpoint(
                        region=region,
                        url=public_url,
                        endpoint_type=OpenStackIdentityEndpointType.EXTERNAL,
                    )
                    entry_endpoints.append(entry_endpoint)

                if private_url:
                    entry_endpoint = OpenStackServiceCatalogEntryEndpoint(
                        region=region,
                        url=private_url,
                        endpoint_type=OpenStackIdentityEndpointType.INTERNAL,
                    )
                    entry_endpoints.append(entry_endpoint)

            entry = OpenStackServiceCatalogEntry(service_type=service, endpoints=entry_endpoints)
            entries.append(entry)

        return entries

    def _parse_service_catalog_auth_v2(self, service_catalog):
        entries = []

        for service in service_catalog:
            service_type = service["type"]
            service_name = service.get("name", None)

            entry_endpoints = []
            for endpoint in service.get("endpoints", []):
                region = endpoint.get("region", None)

                public_url = endpoint.get("publicURL", None)
                private_url = endpoint.get("internalURL", None)

                if public_url:
                    entry_endpoint = OpenStackServiceCatalogEntryEndpoint(
                        region=region,
                        url=public_url,
                        endpoint_type=OpenStackIdentityEndpointType.EXTERNAL,
                    )
                    entry_endpoints.append(entry_endpoint)

                if private_url:
                    entry_endpoint = OpenStackServiceCatalogEntryEndpoint(
                        region=region,
                        url=private_url,
                        endpoint_type=OpenStackIdentityEndpointType.INTERNAL,
                    )
                    entry_endpoints.append(entry_endpoint)

            entry = OpenStackServiceCatalogEntry(
                service_type=service_type,
                endpoints=entry_endpoints,
                service_name=service_name,
            )
            entries.append(entry)

        return entries

    def _parse_service_catalog_auth_v3(self, service_catalog):
        entries = []

        for item in service_catalog:
            service_type = item["type"]
            service_name = item.get("name", None)

            entry_endpoints = []
            for endpoint in item["endpoints"]:
                region = endpoint.get("region", None)
                url = endpoint["url"]
                endpoint_type = endpoint["interface"]

                if endpoint_type == "internal":
                    endpoint_type = OpenStackIdentityEndpointType.INTERNAL
                elif endpoint_type == "public":
                    endpoint_type = OpenStackIdentityEndpointType.EXTERNAL
                elif endpoint_type == "admin":
                    endpoint_type = OpenStackIdentityEndpointType.ADMIN

                entry_endpoint = OpenStackServiceCatalogEntryEndpoint(
                    region=region, url=url, endpoint_type=endpoint_type
                )
                entry_endpoints.append(entry_endpoint)

            entry = OpenStackServiceCatalogEntry(
                service_type=service_type,
                service_name=service_name,
                endpoints=entry_endpoints,
            )
            entries.append(entry)

        return entries


class OpenStackServiceCatalogEntry:
    def __init__(self, service_type, endpoints=None, service_name=None):
        """
        :param service_type: Service type.
        :type service_type: ``str``

        :param endpoints: Endpoints belonging to this entry.
        :type endpoints: ``list``

        :param service_name: Optional service name.
        :type service_name: ``str``
        """
        self.service_type = service_type
        self.endpoints = endpoints or []
        self.service_name = service_name

        # For consistency, sort the endpoints
        self.endpoints = sorted(self.endpoints, key=lambda x: x.url or "")

    def __eq__(self, other):
        return (
            self.service_type == other.service_type
            and self.endpoints == other.endpoints
            and other.service_name == self.service_name
        )

    def __ne__(self, other):
        return not self.__eq__(other=other)

    def __repr__(self):
        return "<OpenStackServiceCatalogEntry service_type=%s, " "service_name=%s, endpoints=%s" % (
            self.service_type,
            self.service_name,
            repr(self.endpoints),
        )


class OpenStackServiceCatalogEntryEndpoint:
    VALID_ENDPOINT_TYPES = [
        OpenStackIdentityEndpointType.INTERNAL,
        OpenStackIdentityEndpointType.EXTERNAL,
        OpenStackIdentityEndpointType.ADMIN,
    ]

    def __init__(self, region, url, endpoint_type="external"):
        """
        :param region: Endpoint region.
        :type region: ``str``

        :param url: Endpoint URL.
        :type url: ``str``

        :param endpoint_type: Endpoint type (external / internal / admin).
        :type endpoint_type: ``str``
        """
        if endpoint_type not in self.VALID_ENDPOINT_TYPES:
            raise ValueError("Invalid type: %s" % (endpoint_type))

        # TODO: Normalize / lowercase all the region names
        self.region = region
        self.url = url
        self.endpoint_type = endpoint_type

    def __eq__(self, other):
        return (
            self.region == other.region
            and self.url == other.url
            and self.endpoint_type == other.endpoint_type
        )

    def __ne__(self, other):
        return not self.__eq__(other=other)

    def __repr__(self):
        return "<OpenStackServiceCatalogEntryEndpoint region=%s, url=%s, " "type=%s" % (
            self.region,
            self.url,
            self.endpoint_type,
        )


class OpenStackAuthResponse(Response):
    def success(self):
        return self.status in [
            httplib.OK,
            httplib.CREATED,
            httplib.ACCEPTED,
            httplib.NO_CONTENT,
            httplib.MULTIPLE_CHOICES,
            httplib.UNAUTHORIZED,
            httplib.INTERNAL_SERVER_ERROR,
        ]

    def parse_body(self):
        if not self.body:
            return None

        if "content-type" in self.headers:
            key = "content-type"
        elif "Content-Type" in self.headers:
            key = "Content-Type"
        else:
            raise LibcloudError("Missing content-type header", driver=OpenStackIdentityConnection)

        content_type = self.headers[key]
        if content_type.find(";") != -1:
            content_type = content_type.split(";")[0]

        if content_type == "application/json":
            try:
                data = json.loads(self.body)
            except Exception:
                driver = OpenStackIdentityConnection
                raise MalformedResponseError("Failed to parse JSON", body=self.body, driver=driver)
        elif content_type == "text/plain":
            data = self.body
        else:
            data = self.body

        return data


class OpenStackIdentityConnection(ConnectionUserAndKey):
    """
    Base identity connection class which contains common / shared logic.

    Note: This class shouldn't be instantiated directly.
    """

    responseCls = OpenStackAuthResponse
    timeout = None
    auth_version = None  # type: str

    def __init__(
        self,
        auth_url,
        user_id,
        key,
        tenant_name=None,
        tenant_domain_id="default",
        domain_name="Default",
        token_scope=OpenStackIdentityTokenScope.PROJECT,
        timeout=None,
        proxy_url=None,
        parent_conn=None,
        auth_cache=None,
    ):
        super().__init__(
            user_id=user_id, key=key, url=auth_url, timeout=timeout, proxy_url=proxy_url
        )

        self.parent_conn = parent_conn

        # enable tests to use the same mock connection classes.
        if parent_conn:
            self.conn_class = parent_conn.conn_class
            self.driver = parent_conn.driver
        else:
            self.driver = None

        self.auth_url = auth_url
        self.tenant_name = tenant_name
        self.domain_name = domain_name
        self.tenant_domain_id = tenant_domain_id
        self.token_scope = token_scope
        self.timeout = timeout
        self.auth_cache = auth_cache

        self.urls = {}
        self.auth_token = None
        self.auth_token_expires = None
        self.auth_user_info = None
        self.auth_user_roles = None

    def authenticated_request(
        self, action, params=None, data=None, headers=None, method="GET", raw=False
    ):
        """
        Perform an authenticated request against the identity API.
        """
        if not self.auth_token:
            raise ValueError("Need to be authenticated to perform this request")

        headers = headers or {}
        headers[AUTH_TOKEN_HEADER] = self.auth_token

        response = self.request(
            action=action,
            params=params,
            data=data,
            headers=headers,
            method=method,
            raw=raw,
        )
        # Evict cached auth token if we receive Unauthorized while using it
        if response.status == httplib.UNAUTHORIZED:
            self.clear_cached_auth_context()
        return response

    def morph_action_hook(self, action):
        (_, _, _, request_path) = self._tuple_from_url(self.auth_url)

        if request_path == "":
            # No path is provided in the auth_url, use action passed to this
            # method.
            return action

        return super().morph_action_hook(action=action)

    def add_default_headers(self, headers):
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json; charset=UTF-8"
        return headers

    def is_token_valid(self):
        """
        Return True if the current auth token is already cached and hasn't
        expired yet.

        :return: ``True`` if the token is still valid, ``False`` otherwise.
        :rtype: ``bool``
        """
        if not self.auth_token:
            return False

        if not self.auth_token_expires:
            return False

        expires = self.auth_token_expires - datetime.timedelta(
            seconds=AUTH_TOKEN_EXPIRES_GRACE_SECONDS
        )

        time_tuple_expires = expires.utctimetuple()
        time_tuple_now = datetime.datetime.utcnow().utctimetuple()

        if time_tuple_now < time_tuple_expires:
            return True

        return False

    def authenticate(self, force=False):
        """
        Authenticate against the identity API.

        :param force: Forcefully update the token even if it's already cached
                      and still valid.
        :type force: ``bool``
        """
        raise NotImplementedError("authenticate not implemented")

    def clear_cached_auth_context(self):
        """
        Clear the cached authentication context.

        The context is cleared from fields on this connection and from the
        external cache, if one is configured.
        """
        self.auth_token = None
        self.auth_token_expires = None
        self.auth_user_info = None
        self.auth_user_roles = None
        self.urls = {}

        if self.auth_cache is not None:
            self.auth_cache.clear(self._cache_key)

    def list_supported_versions(self):
        """
        Retrieve a list of all the identity versions which are supported by
        this installation.

        :rtype: ``list`` of :class:`.OpenStackIdentityVersion`
        """
        response = self.request("/", method="GET")
        result = self._to_versions(data=response.object["versions"]["values"])
        result = sorted(result, key=lambda x: x.version)
        return result

    def _to_versions(self, data):
        result = []
        for item in data:
            version = self._to_version(data=item)
            result.append(version)

        return result

    def _to_version(self, data):
        try:
            updated = parse_date(data["updated"])
        except Exception:
            updated = None

        try:
            url = data["links"][0]["href"]
        except IndexError:
            url = None

        version = OpenStackIdentityVersion(
            version=data["id"], status=data["status"], updated=updated, url=url
        )
        return version

    def _is_authentication_needed(self, force=False):
        """
        Determine if the authentication is needed or if the existing token (if
        any exists) is still valid.
        """
        if force:
            return True

        if self.auth_version not in AUTH_VERSIONS_WITH_EXPIRES:
            return True

        if self.is_token_valid():
            return False

        # See if there's a new token in the cache
        self._load_auth_context_from_cache()

        # If there was a token in the cache, it is now stored in our local
        # auth_token and related fields.  Ensure it is still valid.
        if self.is_token_valid():
            return False

        return True

    def _to_projects(self, data):
        result = []
        for item in data:
            project = self._to_project(data=item)
            result.append(project)

        return result

    def _to_project(self, data):
        project = OpenStackIdentityProject(
            id=data["id"],
            name=data["name"],
            description=data["description"],
            enabled=data["enabled"],
            domain_id=data.get("domain_id", None),
        )
        return project

    @property
    def _cache_key(self):
        """
        The key where this connection's authentication context will be cached.

        :rtype: :class:`OpenStackAuthenticationCacheKey`
        """
        return OpenStackAuthenticationCacheKey(
            self.auth_url,
            self.user_id,
            self.token_scope,
            self.tenant_name,
            self.domain_name,
            self.tenant_domain_id,
        )

    def _cache_auth_context(self, context):
        """
        Store an authentication context in memory and the cache.

        :param context: Authentication context to cache.
        :type key: :class:`.OpenStackAuthenticationContext`
        """
        self.urls = context.urls
        self.auth_token = context.token
        self.auth_token_expires = context.expiration
        self.auth_user_info = context.user
        self.auth_user_roles = context.roles

        if self.auth_cache is not None:
            self.auth_cache.put(self._cache_key, context)

    def _load_auth_context_from_cache(self):
        """
        Fetch an authentication context for this connection from the cache.

        :rtype: :class:`OpenStackAuthenticationContext`
        """
        if self.auth_cache is None:
            return None

        context = self.auth_cache.get(self._cache_key)
        if context is None:
            return None

        self.urls = context.urls
        self.auth_token = context.token
        self.auth_token_expires = context.expiration
        self.auth_user_info = context.user
        self.auth_user_roles = context.roles
        return context


class OpenStackIdentity_1_0_Connection(OpenStackIdentityConnection):
    """
    Connection class for Keystone API v1.0.
    """

    responseCls = OpenStackAuthResponse
    name = "OpenStack Identity API v1.0"
    auth_version = "1.0"

    def authenticate(self, force=False):
        if not self._is_authentication_needed(force=force):
            return self

        headers = {
            "X-Auth-User": self.user_id,
            "X-Auth-Key": self.key,
        }

        resp = self.request("/v1.0", headers=headers, method="GET")

        if resp.status == httplib.UNAUTHORIZED:
            # HTTP UNAUTHORIZED (401): auth failed
            raise InvalidCredsError()
        elif resp.status not in [httplib.NO_CONTENT, httplib.OK]:
            body = "code: {} body:{} headers:{}".format(
                resp.status,
                resp.body,
                resp.headers,
            )
            raise MalformedResponseError("Malformed response", body=body, driver=self.driver)
        else:
            headers = resp.headers
            # emulate the auth 1.1 URL list
            self.urls = {}
            self.urls["cloudServers"] = [
                {"publicURL": headers.get("x-server-management-url", None)}
            ]
            self.urls["cloudFilesCDN"] = [{"publicURL": headers.get("x-cdn-management-url", None)}]
            self.urls["cloudFiles"] = [{"publicURL": headers.get("x-storage-url", None)}]
            self.auth_token = headers.get("x-auth-token", None)
            self.auth_user_info = None

            if not self.auth_token:
                raise MalformedResponseError("Missing X-Auth-Token in" " response headers")

        return self


class OpenStackIdentity_1_1_Connection(OpenStackIdentityConnection):
    """
    Connection class for Keystone API v1.1.
    """

    responseCls = OpenStackAuthResponse
    name = "OpenStack Identity API v1.1"
    auth_version = "1.1"

    def authenticate(self, force=False):
        if not self._is_authentication_needed(force=force):
            return self

        reqbody = json.dumps({"credentials": {"username": self.user_id, "key": self.key}})
        resp = self.request("/v1.1/auth", data=reqbody, headers={}, method="POST")

        if resp.status == httplib.UNAUTHORIZED:
            # HTTP UNAUTHORIZED (401): auth failed
            raise InvalidCredsError()
        elif resp.status != httplib.OK:
            body = "code: {} body:{}".format(resp.status, resp.body)
            raise MalformedResponseError("Malformed response", body=body, driver=self.driver)
        else:
            try:
                body = json.loads(resp.body)
            except Exception as e:
                raise MalformedResponseError("Failed to parse JSON", e)

            try:
                expires = body["auth"]["token"]["expires"]
                self._cache_auth_context(
                    OpenStackAuthenticationContext(
                        body["auth"]["token"]["id"],
                        expiration=parse_date(expires),
                        urls=body["auth"]["serviceCatalog"],
                    )
                )
            except KeyError as e:
                raise MalformedResponseError(
                    "Auth JSON response is \
                                             missing required elements",
                    e,
                )

        return self


class OpenStackIdentity_2_0_Connection(OpenStackIdentityConnection):
    """
    Connection class for Keystone API v2.0.
    """

    responseCls = OpenStackAuthResponse
    name = "OpenStack Identity API v1.0"
    auth_version = "2.0"

    def authenticate(self, auth_type="api_key", force=False):
        if not self._is_authentication_needed(force=force):
            return self

        if auth_type == "api_key":
            return self._authenticate_2_0_with_api_key()
        elif auth_type == "password":
            return self._authenticate_2_0_with_password()
        else:
            raise ValueError("Invalid value for auth_type argument")

    def _authenticate_2_0_with_api_key(self):
        # API Key based authentication uses the RAX-KSKEY extension.
        # http://s.apache.org/oAi
        data = {
            "auth": {
                "RAX-KSKEY:apiKeyCredentials": {
                    "username": self.user_id,
                    "apiKey": self.key,
                }
            }
        }
        if self.tenant_name:
            data["auth"]["tenantName"] = self.tenant_name
        reqbody = json.dumps(data)
        return self._authenticate_2_0_with_body(reqbody)

    def _authenticate_2_0_with_password(self):
        # Password based authentication is the only 'core' authentication
        # method in Keystone at this time.
        # 'keystone' - http://s.apache.org/e8h
        data = {"auth": {"passwordCredentials": {"username": self.user_id, "password": self.key}}}
        if self.tenant_name:
            data["auth"]["tenantName"] = self.tenant_name
        reqbody = json.dumps(data)
        return self._authenticate_2_0_with_body(reqbody)

    def _authenticate_2_0_with_body(self, reqbody):
        resp = self.request(
            "/v2.0/tokens",
            data=reqbody,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        if resp.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError()
        elif resp.status not in [httplib.OK, httplib.NON_AUTHORITATIVE_INFORMATION]:
            body = "code: {} body: {}".format(resp.status, resp.body)
            raise MalformedResponseError("Malformed response", body=body, driver=self.driver)
        else:
            body = resp.object

            try:
                access = body["access"]
                expires = access["token"]["expires"]
                self._cache_auth_context(
                    OpenStackAuthenticationContext(
                        access["token"]["id"],
                        expiration=parse_date(expires),
                        urls=access["serviceCatalog"],
                        user=access.get("user", {}),
                    )
                )
            except KeyError as e:
                raise MalformedResponseError(
                    "Auth JSON response is \
                                             missing required elements",
                    e,
                )

        return self

    def list_projects(self):
        response = self.authenticated_request("/v2.0/tenants", method="GET")
        result = self._to_projects(data=response.object["tenants"])
        return result

    def list_tenants(self):
        return self.list_projects()


class OpenStackIdentity_3_0_Connection(OpenStackIdentityConnection):
    """
    Connection class for Keystone API v3.x.
    """

    responseCls = OpenStackAuthResponse
    name = "OpenStack Identity API v3.x"
    auth_version = "3.0"

    VALID_TOKEN_SCOPES = [
        OpenStackIdentityTokenScope.PROJECT,
        OpenStackIdentityTokenScope.DOMAIN,
        OpenStackIdentityTokenScope.UNSCOPED,
    ]

    def __init__(
        self,
        auth_url,
        user_id,
        key,
        tenant_name=None,
        domain_name="Default",
        tenant_domain_id="default",
        token_scope=OpenStackIdentityTokenScope.PROJECT,
        timeout=None,
        proxy_url=None,
        parent_conn=None,
        auth_cache=None,
    ):
        """
        :param tenant_name: Name of the project this user belongs to. Note:
                            When token_scope is set to project, this argument
                            control to which project to scope the token to.
        :type tenant_name: ``str``

        :param domain_name: Domain the user belongs to. Note: When token_scope
                            is set to token, this argument controls to which
                            domain to scope the token to.
        :type domain_name: ``str``

        :param token_scope: Whether to scope a token to a "project", a
                            "domain" or "unscoped"
        :type token_scope: ``str``

        :param auth_cache: Where to cache authentication tokens.
        :type auth_cache: :class:`OpenStackAuthenticationCache`
        """
        super().__init__(
            auth_url=auth_url,
            user_id=user_id,
            key=key,
            tenant_name=tenant_name,
            domain_name=domain_name,
            tenant_domain_id=tenant_domain_id,
            token_scope=token_scope,
            timeout=timeout,
            proxy_url=proxy_url,
            parent_conn=parent_conn,
            auth_cache=auth_cache,
        )

        if self.token_scope not in self.VALID_TOKEN_SCOPES:
            raise ValueError('Invalid value for "token_scope" argument: %s' % (self.token_scope))

        if self.token_scope == OpenStackIdentityTokenScope.PROJECT and (
            not self.tenant_name or not self.domain_name
        ):
            raise ValueError("Must provide tenant_name and domain_name " "argument")
        elif self.token_scope == OpenStackIdentityTokenScope.DOMAIN and not self.domain_name:
            raise ValueError("Must provide domain_name argument")

    def authenticate(self, force=False):
        """
        Perform authentication.
        """
        if not self._is_authentication_needed(force=force):
            return self

        data = self._get_auth_data()
        data = json.dumps(data)
        response = self.request(
            "/v3/auth/tokens",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        self._parse_token_response(response, cache_it=True)
        return self

    def list_domains(self):
        """
        List the available domains.

        :rtype: ``list`` of :class:`OpenStackIdentityDomain`
        """
        response = self.authenticated_request("/v3/domains", method="GET")
        result = self._to_domains(data=response.object["domains"])
        return result

    def list_projects(self):
        """
        List the available projects.

        Note: To perform this action, user you are currently authenticated with
        needs to be an admin.

        :rtype: ``list`` of :class:`OpenStackIdentityProject`
        """
        response = self.authenticated_request("/v3/projects", method="GET")
        result = self._to_projects(data=response.object["projects"])
        return result

    def list_users(self):
        """
        List the available users.

        :rtype: ``list`` of :class:`.OpenStackIdentityUser`
        """
        response = self.authenticated_request("/v3/users", method="GET")
        result = self._to_users(data=response.object["users"])
        return result

    def list_roles(self):
        """
        List the available roles.

        :rtype: ``list`` of :class:`.OpenStackIdentityRole`
        """
        response = self.authenticated_request("/v3/roles", method="GET")
        result = self._to_roles(data=response.object["roles"])
        return result

    def get_domain(self, domain_id):
        """
        Retrieve information about a single domain.

        :param domain_id: ID of domain to retrieve information for.
        :type domain_id: ``str``

        :rtype: :class:`.OpenStackIdentityDomain`
        """
        response = self.authenticated_request("/v3/domains/%s" % (domain_id), method="GET")
        result = self._to_domain(data=response.object["domain"])
        return result

    def get_user(self, user_id):
        """
        Get a user account by ID.

        :param user_id: User's id.
        :type name: ``str``

        :return: Located user.
        :rtype: :class:`.OpenStackIdentityUser`
        """
        response = self.authenticated_request("/v3/users/%s" % user_id)
        user = self._to_user(data=response.object["user"])
        return user

    def list_user_projects(self, user):
        """
        Retrieve all the projects user belongs to.

        :rtype: ``list`` of :class:`.OpenStackIdentityProject`
        """
        path = "/v3/users/%s/projects" % (user.id)
        response = self.authenticated_request(path, method="GET")
        result = self._to_projects(data=response.object["projects"])
        return result

    def list_user_domain_roles(self, domain, user):
        """
        Retrieve all the roles for a particular user on a domain.

        :rtype: ``list`` of :class:`.OpenStackIdentityRole`
        """
        # TODO: Also add "get users roles" and "get assginements" which are
        # available in 3.1 and 3.3
        path = "/v3/domains/{}/users/{}/roles".format(domain.id, user.id)
        response = self.authenticated_request(path, method="GET")
        result = self._to_roles(data=response.object["roles"])
        return result

    def grant_domain_role_to_user(self, domain, role, user):
        """
        Grant domain role to a user.

        Note: This function appears to be idempotent.

        :param domain: Domain to grant the role to.
        :type domain: :class:`.OpenStackIdentityDomain`

        :param role: Role to grant.
        :type role: :class:`.OpenStackIdentityRole`

        :param user: User to grant the role to.
        :type user: :class:`.OpenStackIdentityUser`

        :return: ``True`` on success.
        :rtype: ``bool``
        """
        path = "/v3/domains/{}/users/{}/roles/{}".format(domain.id, user.id, role.id)
        response = self.authenticated_request(path, method="PUT")
        return response.status == httplib.NO_CONTENT

    def revoke_domain_role_from_user(self, domain, user, role):
        """
        Revoke domain role from a user.

        :param domain: Domain to revoke the role from.
        :type domain: :class:`.OpenStackIdentityDomain`

        :param role: Role to revoke.
        :type role: :class:`.OpenStackIdentityRole`

        :param user: User to revoke the role from.
        :type user: :class:`.OpenStackIdentityUser`

        :return: ``True`` on success.
        :rtype: ``bool``
        """
        path = "/v3/domains/{}/users/{}/roles/{}".format(domain.id, user.id, role.id)
        response = self.authenticated_request(path, method="DELETE")
        return response.status == httplib.NO_CONTENT

    def grant_project_role_to_user(self, project, role, user):
        """
        Grant project role to a user.

        Note: This function appears to be idempotent.

        :param project: Project to grant the role to.
        :type project: :class:`.OpenStackIdentityDomain`

        :param role: Role to grant.
        :type role: :class:`.OpenStackIdentityRole`

        :param user: User to grant the role to.
        :type user: :class:`.OpenStackIdentityUser`

        :return: ``True`` on success.
        :rtype: ``bool``
        """
        path = "/v3/projects/{}/users/{}/roles/{}".format(project.id, user.id, role.id)
        response = self.authenticated_request(path, method="PUT")
        return response.status == httplib.NO_CONTENT

    def revoke_project_role_from_user(self, project, role, user):
        """
        Revoke project role from a user.

        :param project: Project to revoke the role from.
        :type project: :class:`.OpenStackIdentityDomain`

        :param role: Role to revoke.
        :type role: :class:`.OpenStackIdentityRole`

        :param user: User to revoke the role from.
        :type user: :class:`.OpenStackIdentityUser`

        :return: ``True`` on success.
        :rtype: ``bool``
        """
        path = "/v3/projects/{}/users/{}/roles/{}".format(project.id, user.id, role.id)
        response = self.authenticated_request(path, method="DELETE")
        return response.status == httplib.NO_CONTENT

    def create_user(
        self,
        email,
        password,
        name,
        description=None,
        domain_id=None,
        default_project_id=None,
        enabled=True,
    ):
        """
        Create a new user account.

        :param email: User's mail address.
        :type email: ``str``

        :param password: User's password.
        :type password: ``str``

        :param name: User's name.
        :type name: ``str``

        :param description: Optional description.
        :type description: ``str``

        :param domain_id: ID of the domain to add the user to (optional).
        :type domain_id: ``str``

        :param default_project_id: ID of the default user project (optional).
        :type default_project_id: ``str``

        :param enabled: True to enable user after creation.
        :type enabled: ``bool``

        :return: Created user.
        :rtype: :class:`.OpenStackIdentityUser`
        """
        data = {"email": email, "password": password, "name": name, "enabled": enabled}

        if description:
            data["description"] = description

        if domain_id:
            data["domain_id"] = domain_id

        if default_project_id:
            data["default_project_id"] = default_project_id

        data = json.dumps({"user": data})
        response = self.authenticated_request("/v3/users", data=data, method="POST")

        user = self._to_user(data=response.object["user"])
        return user

    def enable_user(self, user):
        """
        Enable user account.

        Note: This operation appears to be idempotent.

        :param user: User to enable.
        :type user: :class:`.OpenStackIdentityUser`

        :return: User account which has been enabled.
        :rtype: :class:`.OpenStackIdentityUser`
        """
        data = {"enabled": True}
        data = json.dumps({"user": data})
        response = self.authenticated_request("/v3/users/%s" % (user.id), data=data, method="PATCH")

        user = self._to_user(data=response.object["user"])
        return user

    def disable_user(self, user):
        """
        Disable user account.

        Note: This operation appears to be idempotent.

        :param user: User to disable.
        :type user: :class:`.OpenStackIdentityUser`

        :return: User account which has been disabled.
        :rtype: :class:`.OpenStackIdentityUser`
        """
        data = {"enabled": False}
        data = json.dumps({"user": data})
        response = self.authenticated_request("/v3/users/%s" % (user.id), data=data, method="PATCH")

        user = self._to_user(data=response.object["user"])
        return user

    def _get_auth_data(self):
        data = {
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "domain": {"name": self.domain_name},
                            "name": self.user_id,
                            "password": self.key,
                        }
                    },
                }
            }
        }

        if self.token_scope == OpenStackIdentityTokenScope.PROJECT:
            # Scope token to project (tenant)
            data["auth"]["scope"] = {
                "project": {
                    "domain": {"id": self.tenant_domain_id},
                    "name": self.tenant_name,
                }
            }
        elif self.token_scope == OpenStackIdentityTokenScope.DOMAIN:
            # Scope token to domain
            data["auth"]["scope"] = {"domain": {"name": self.domain_name}}
        elif self.token_scope == OpenStackIdentityTokenScope.UNSCOPED:
            pass
        else:
            raise ValueError("Token needs to be scoped either to project or " "a domain")

        return data

    def _load_auth_context_from_cache(self):
        context = super()._load_auth_context_from_cache()
        if context is None:
            return None

        # Since v3 only caches the token and expiration, fetch the
        # service catalog and other bits of the authentication context
        # from Keystone.
        try:
            self._fetch_auth_token()
        except InvalidCredsError:
            # Unauthorized; cached auth context was cleared as part of
            # _fetch_auth_token
            return None

        # Local auth context variables set in _fetch_auth_token
        return context

    def _parse_token_response(self, response, cache_it=False, raise_ambiguous_version_error=True):
        """
        Parse a response from /v3/auth/tokens.

        :param cache_it: Should we cache the authentication context?
        :type cache_it: ``bool``

        :param raise_ambiguous_version_error: Should an ambiguous version
            error be raised on a 300 response?
        :type raise_ambiguous_version_error: ``bool``
        """
        if response.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError()
        elif response.status in [httplib.OK, httplib.CREATED]:
            headers = response.headers

            try:
                body = json.loads(response.body)
            except Exception as e:
                raise MalformedResponseError("Failed to parse JSON", e)

            try:
                roles = self._to_roles(body["token"]["roles"])
            except Exception:
                roles = []

            try:
                expires = parse_date(body["token"]["expires_at"])
                token = headers["x-subject-token"]

                # Cache the fewest fields required for token reuse to minimize
                # cache size. Other fields, especially the service catalog, can
                # be quite large. Fetch these from Keystone when the token is
                # first loaded from cache.
                if cache_it:
                    self._cache_auth_context(
                        OpenStackAuthenticationContext(token, expiration=expires)
                    )

                self.auth_token = token
                self.auth_token_expires = expires
                # Note: catalog is not returned for unscoped tokens
                self.urls = body["token"].get("catalog", None)
                self.auth_user_info = body["token"].get("user", None)
                self.auth_user_roles = roles
            except KeyError as e:
                raise MalformedResponseError(
                    "Auth JSON response is \
                                             missing required elements",
                    e,
                )
        elif raise_ambiguous_version_error and response.status == 300:
            # ambiguous version request
            raise LibcloudError(
                "Auth request returned ambiguous version error, try"
                "using the version specific URL to connect,"
                " e.g. identity/v3/auth/tokens"
            )
        else:
            body = "code: {} body:{}".format(response.status, response.body)
            raise MalformedResponseError("Malformed response", body=body, driver=self.driver)

    def _fetch_auth_token(self):
        """
        Fetch our authentication token and service catalog.
        """
        headers = {"X-Subject-Token": self.auth_token}
        response = self.authenticated_request("/v3/auth/tokens", headers=headers)
        self._parse_token_response(response)
        return self

    def _to_domains(self, data):
        result = []
        for item in data:
            domain = self._to_domain(data=item)
            result.append(domain)

        return result

    def _to_domain(self, data):
        domain = OpenStackIdentityDomain(id=data["id"], name=data["name"], enabled=data["enabled"])
        return domain

    def _to_users(self, data):
        result = []
        for item in data:
            user = self._to_user(data=item)
            result.append(user)

        return result

    def _to_user(self, data):
        user = OpenStackIdentityUser(
            id=data["id"],
            domain_id=data["domain_id"],
            name=data["name"],
            email=data.get("email"),
            description=data.get("description", None),
            enabled=data.get("enabled"),
        )
        return user

    def _to_roles(self, data):
        result = []
        for item in data:
            user = self._to_role(data=item)
            result.append(user)

        return result

    def _to_role(self, data):
        role = OpenStackIdentityRole(
            id=data["id"],
            name=data["name"],
            description=data.get("description", None),
            enabled=data.get("enabled", True),
        )
        return role


class OpenStackIdentity_3_0_Connection_AppCred(OpenStackIdentity_3_0_Connection):
    """
    Connection class for Keystone API v3.x using Application Credentials.

    'user_id' is the application credential id and 'key' is the application
    credential secret.
    """

    name = "OpenStack Identity API v3.x with Application Credentials"

    def __init__(
        self,
        auth_url,
        user_id,
        key,
        tenant_name=None,
        domain_name=None,
        tenant_domain_id=None,
        token_scope=None,
        timeout=None,
        proxy_url=None,
        parent_conn=None,
        auth_cache=None,
    ):
        """
        Tenant, domain and scope options are ignored as they are contained
        within the app credential itself and can't be changed.
        """
        super().__init__(
            auth_url=auth_url,
            user_id=user_id,
            key=key,
            tenant_name=tenant_name,
            domain_name=domain_name,
            token_scope=OpenStackIdentityTokenScope.UNSCOPED,
            timeout=timeout,
            proxy_url=proxy_url,
            parent_conn=parent_conn,
            auth_cache=auth_cache,
        )

    def _get_auth_data(self):
        data = {
            "auth": {
                "identity": {
                    "methods": ["application_credential"],
                    "application_credential": {"id": self.user_id, "secret": self.key},
                }
            }
        }
        return data


class OpenStackIdentity_3_0_Connection_OIDC_access_token(OpenStackIdentity_3_0_Connection):
    """
    Connection class for Keystone API v3.x. using OpenID Connect tokens

    The OIDC token must be set in the self.key attribute.

    The identity provider name required to get the full path
    must be set in the self.user_id attribute.

    The protocol name required to get the full path
    must be set in the self.tenant_name attribute.

    The self.domain_name attribute can be used either to select the
    domain name in case of domain scoped token or to select the project
    name in case of project scoped token
    """

    responseCls = OpenStackAuthResponse
    name = "OpenStack Identity API v3.x with OIDC support"
    auth_version = "3.0"

    def authenticate(self, force=False):
        """
        Perform authentication.
        """
        if not self._is_authentication_needed(force=force):
            return self

        subject_token = self._get_unscoped_token_from_oidc_token()

        data = {"auth": {"identity": {"methods": ["token"], "token": {"id": subject_token}}}}

        if self.token_scope == OpenStackIdentityTokenScope.PROJECT:
            # Scope token to project (tenant)
            project_id = self._get_project_id(token=subject_token)
            data["auth"]["scope"] = {"project": {"id": project_id}}
        elif self.token_scope == OpenStackIdentityTokenScope.DOMAIN:
            # Scope token to domain
            data["auth"]["scope"] = {"domain": {"name": self.domain_name}}
        elif self.token_scope == OpenStackIdentityTokenScope.UNSCOPED:
            pass
        else:
            raise ValueError("Token needs to be scoped either to project or " "a domain")

        data = json.dumps(data)
        response = self.request(
            "/v3/auth/tokens",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        self._parse_token_response(response, cache_it=True, raise_ambiguous_version_error=False)
        return self

    def _get_unscoped_token_from_oidc_token(self):
        """
        Get unscoped token from OIDC access token
        """
        path = "/v3/OS-FEDERATION/identity_providers/{}/protocols/{}/auth".format(
            self.user_id,
            self.tenant_name,
        )
        response = self.request(
            path,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer %s" % self.key,
            },
            method="GET",
        )

        if response.status == httplib.UNAUTHORIZED:
            # Invalid credentials
            raise InvalidCredsError()
        elif response.status in [httplib.OK, httplib.CREATED]:
            if "x-subject-token" in response.headers:
                return response.headers["x-subject-token"]
            else:
                raise MalformedResponseError("No x-subject-token returned", driver=self.driver)
        else:
            raise MalformedResponseError(
                "Malformed response", driver=self.driver, body=response.body
            )

    def _get_project_id(self, token):
        """
        Get the first project ID accessible with the specified access token
        """
        # Try new path first (from ver 1.1)
        path = "/v3/auth/projects"
        response = self.request(
            path,
            headers={"Content-Type": "application/json", AUTH_TOKEN_HEADER: token},
            method="GET",
        )

        if response.status not in [httplib.UNAUTHORIZED, httplib.OK, httplib.CREATED]:
            # In case of error try old one
            path = "/v3/OS-FEDERATION/projects"
            response = self.request(
                path,
                headers={"Content-Type": "application/json", AUTH_TOKEN_HEADER: token},
                method="GET",
            )

        if response.status == httplib.UNAUTHORIZED:
            # Invalid credentials
            raise InvalidCredsError()
        elif response.status in [httplib.OK, httplib.CREATED]:
            try:
                body = json.loads(response.body)
                # We use domain_name in both cases of the scoped tokens
                # as we have used tenant as the protocol
                if self.domain_name and self.domain_name != "Default":
                    for project in body["projects"]:
                        if self.domain_name in [project["name"], project["id"]]:
                            return project["id"]
                    raise ValueError("Project %s not found" % (self.domain_name))
                else:
                    return body["projects"][0]["id"]
            except ValueError as e:
                raise e
            except Exception as e:
                raise MalformedResponseError("Failed to parse JSON", e)
        else:
            raise MalformedResponseError(
                "Malformed response", driver=self.driver, body=response.body
            )


class OpenStackIdentity_2_0_Connection_VOMS(OpenStackIdentityConnection, CertificateConnection):
    """
    Connection class for Keystone API v2.0. with VOMS proxy support
    In this case the key parameter will be the path of the VOMS proxy file.
    """

    responseCls = OpenStackAuthResponse
    name = "OpenStack Identity API v2.0 VOMS support"
    auth_version = "2.0"

    def __init__(
        self,
        auth_url,
        user_id,
        key,
        tenant_name=None,
        tenant_domain_id="default",
        domain_name="Default",
        token_scope=OpenStackIdentityTokenScope.PROJECT,
        timeout=None,
        proxy_url=None,
        parent_conn=None,
        auth_cache=None,
    ):
        CertificateConnection.__init__(
            self, cert_file=key, url=auth_url, proxy_url=proxy_url, timeout=timeout
        )

        self.parent_conn = parent_conn

        # enable tests to use the same mock connection classes.
        if parent_conn:
            self.conn_class = parent_conn.conn_class
            self.driver = parent_conn.driver
        else:
            self.driver = None

        self.auth_url = auth_url
        self.tenant_name = tenant_name
        self.tenant_domain_id = tenant_domain_id
        self.domain_name = domain_name
        self.token_scope = token_scope
        self.timeout = timeout
        self.proxy_url = proxy_url
        self.auth_cache = auth_cache

        self.urls = {}
        self.auth_token = None
        self.auth_token_expires = None
        self.auth_user_info = None

    def authenticate(self, force=False):
        if not self._is_authentication_needed(force=force):
            return self

        tenant = self.tenant_name
        if not tenant:
            # if the tenant name is not specified look for it
            token = self._get_unscoped_token()
            tenant = self._get_tenant_name(token)

        data = {"auth": {"voms": True, "tenantName": tenant}}

        reqbody = json.dumps(data)
        return self._authenticate_2_0_with_body(reqbody)

    def _get_unscoped_token(self):
        """
        Get unscoped token from VOMS proxy
        """
        data = {"auth": {"voms": True}}
        reqbody = json.dumps(data)

        response = self.request(
            "/v2.0/tokens",
            data=reqbody,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        if response.status == httplib.UNAUTHORIZED:
            # Invalid credentials
            raise InvalidCredsError()
        elif response.status in [httplib.OK, httplib.CREATED]:
            try:
                body = json.loads(response.body)
                return body["access"]["token"]["id"]
            except Exception as e:
                raise MalformedResponseError("Failed to parse JSON", e)
        else:
            raise MalformedResponseError(
                "Malformed response", driver=self.driver, body=response.body
            )

    def _get_tenant_name(self, token):
        """
        Get the first available tenant name (usually there are only one)
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            AUTH_TOKEN_HEADER: token,
        }
        response = self.request("/v2.0/tenants", headers=headers, method="GET")

        if response.status == httplib.UNAUTHORIZED:
            # Invalid credentials
            raise InvalidCredsError()
        elif response.status in [httplib.OK, httplib.CREATED]:
            try:
                body = json.loads(response.body)
                return body["tenants"][0]["name"]
            except Exception as e:
                raise MalformedResponseError("Failed to parse JSON", e)
        else:
            raise MalformedResponseError(
                "Malformed response", driver=self.driver, body=response.body
            )

    def _authenticate_2_0_with_body(self, reqbody):
        resp = self.request(
            "/v2.0/tokens",
            data=reqbody,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        if resp.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError()
        elif resp.status not in [httplib.OK, httplib.NON_AUTHORITATIVE_INFORMATION]:
            body = "code: {} body: {}".format(resp.status, resp.body)
            raise MalformedResponseError("Malformed response", body=body, driver=self.driver)
        else:
            body = resp.object

            try:
                access = body["access"]
                expires = access["token"]["expires"]
                self._cache_auth_context(
                    OpenStackAuthenticationContext(
                        access["token"]["id"],
                        expiration=parse_date(expires),
                        urls=access["serviceCatalog"],
                        user=access.get("user", {}),
                    )
                )
            except KeyError as e:
                raise MalformedResponseError(
                    "Auth JSON response is \
                                             missing required elements",
                    e,
                )
        return self


def get_class_for_auth_version(auth_version):
    """
    Retrieve class for the provided auth version.
    """
    if auth_version == "1.0":
        cls = OpenStackIdentity_1_0_Connection
    elif auth_version == "1.1":
        cls = OpenStackIdentity_1_1_Connection
    elif auth_version == "2.0" or auth_version == "2.0_apikey":
        cls = OpenStackIdentity_2_0_Connection
    elif auth_version == "2.0_password":
        cls = OpenStackIdentity_2_0_Connection
    elif auth_version == "2.0_voms":
        cls = OpenStackIdentity_2_0_Connection_VOMS
    elif auth_version == "3.x_password":
        cls = OpenStackIdentity_3_0_Connection
    elif auth_version == "3.x_appcred":
        cls = OpenStackIdentity_3_0_Connection_AppCred
    elif auth_version == "3.x_oidc_access_token":
        cls = OpenStackIdentity_3_0_Connection_OIDC_access_token
    else:
        raise LibcloudError("Unsupported Auth Version requested: %s" % (auth_version))

    return cls
