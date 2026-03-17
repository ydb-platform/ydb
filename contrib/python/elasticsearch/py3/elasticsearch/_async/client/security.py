#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import typing as t

from elastic_transport import ObjectApiResponse

from ._base import NamespacedClient
from .utils import SKIP_IN_PATH, _quote, _rewrite_parameters


class SecurityClient(NamespacedClient):

    @_rewrite_parameters(
        body_fields=("grant_type", "access_token", "password", "username"),
    )
    async def activate_user_profile(
        self,
        *,
        grant_type: t.Optional[
            t.Union[str, t.Literal["access_token", "password"]]
        ] = None,
        access_token: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        password: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        username: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Activate a user profile.</p>
          <p>Create or update a user profile on behalf of another user.</p>
          <p>NOTE: The user profile feature is designed only for use by Kibana and Elastic's Observability, Enterprise Search, and Elastic Security solutions.
          Individual users and external applications should not call this API directly.
          The calling application must have either an <code>access_token</code> or a combination of <code>username</code> and <code>password</code> for the user that the profile document is intended for.
          Elastic reserves the right to change or remove this feature in future releases without prior notice.</p>
          <p>This API creates or updates a profile document for end users with information that is extracted from the user's authentication object including <code>username</code>, <code>full_name,</code> <code>roles</code>, and the authentication realm.
          For example, in the JWT <code>access_token</code> case, the profile user's <code>username</code> is extracted from the JWT token claim pointed to by the <code>claims.principal</code> setting of the JWT realm that authenticated the token.</p>
          <p>When updating a profile document, the API enables the document if it was disabled.
          Any updates do not change existing content for either the <code>labels</code> or <code>data</code> fields.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-activate-user-profile>`_

        :param grant_type: The type of grant.
        :param access_token: The user's Elasticsearch access token or JWT. Both `access`
            and `id` JWT token types are supported and they depend on the underlying
            JWT realm configuration. If you specify the `access_token` grant type, this
            parameter is required. It is not valid with other grant types.
        :param password: The user's password. If you specify the `password` grant type,
            this parameter is required. It is not valid with other grant types.
        :param username: The username that identifies the user. If you specify the `password`
            grant type, this parameter is required. It is not valid with other grant
            types.
        """
        if grant_type is None and body is None:
            raise ValueError("Empty value passed for parameter 'grant_type'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/profile/_activate"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if grant_type is not None:
                __body["grant_type"] = grant_type
            if access_token is not None:
                __body["access_token"] = access_token
            if password is not None:
                __body["password"] = password
            if username is not None:
                __body["username"] = username
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.activate_user_profile",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def authenticate(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Authenticate a user.</p>
          <p>Authenticates a user and returns information about the authenticated user.
          Include the user information in a <a href="https://en.wikipedia.org/wiki/Basic_access_authentication">basic auth header</a>.
          A successful call returns a JSON structure that shows user information such as their username, the roles that are assigned to the user, any assigned metadata, and information about the realms that authenticated and authorized the user.
          If the user cannot be authenticated, this API returns a 401 status code.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-authenticate>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/_authenticate"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.authenticate",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("names",),
    )
    async def bulk_delete_role(
        self,
        *,
        names: t.Optional[t.Sequence[str]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Bulk delete roles.</p>
          <p>The role management APIs are generally the preferred way to manage roles, rather than using file-based role management.
          The bulk delete roles API cannot delete roles that are defined in roles files.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-bulk-delete-role>`_

        :param names: An array of role names to delete
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if names is None and body is None:
            raise ValueError("Empty value passed for parameter 'names'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/role"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if names is not None:
                __body["names"] = names
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.bulk_delete_role",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("roles",),
    )
    async def bulk_put_role(
        self,
        *,
        roles: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Bulk create or update roles.</p>
          <p>The role management APIs are generally the preferred way to manage roles, rather than using file-based role management.
          The bulk create or update roles API cannot update roles that are defined in roles files.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-bulk-put-role>`_

        :param roles: A dictionary of role name to RoleDescriptor objects to add or update
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if roles is None and body is None:
            raise ValueError("Empty value passed for parameter 'roles'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/role"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if roles is not None:
                __body["roles"] = roles
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.bulk_put_role",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("ids", "expiration", "metadata", "role_descriptors"),
    )
    async def bulk_update_api_keys(
        self,
        *,
        ids: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        expiration: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        role_descriptors: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Bulk update API keys.
          Update the attributes for multiple API keys.</p>
          <p>IMPORTANT: It is not possible to use an API key as the authentication credential for this API. To update API keys, the owner user's credentials are required.</p>
          <p>This API is similar to the update API key API but enables you to apply the same update to multiple API keys in one API call. This operation can greatly improve performance over making individual updates.</p>
          <p>It is not possible to update expired or invalidated API keys.</p>
          <p>This API supports updates to API key access scope, metadata and expiration.
          The access scope of each API key is derived from the <code>role_descriptors</code> you specify in the request and a snapshot of the owner user's permissions at the time of the request.
          The snapshot of the owner's permissions is updated automatically on every call.</p>
          <p>IMPORTANT: If you don't specify <code>role_descriptors</code> in the request, a call to this API might still change an API key's access scope. This change can occur if the owner user's permissions have changed since the API key was created or last modified.</p>
          <p>A successful request returns a JSON structure that contains the IDs of all updated API keys, the IDs of API keys that already had the requested changes and did not require an update, and error details for any failed update.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-bulk-update-api-keys>`_

        :param ids: The API key identifiers.
        :param expiration: Expiration time for the API keys. By default, API keys never
            expire. This property can be omitted to leave the value unchanged.
        :param metadata: Arbitrary nested metadata to associate with the API keys. Within
            the `metadata` object, top-level keys beginning with an underscore (`_`)
            are reserved for system usage. Any information specified with this parameter
            fully replaces metadata previously associated with the API key.
        :param role_descriptors: The role descriptors to assign to the API keys. An API
            key's effective permissions are an intersection of its assigned privileges
            and the point-in-time snapshot of permissions of the owner user. You can
            assign new privileges by specifying them in this parameter. To remove assigned
            privileges, supply the `role_descriptors` parameter as an empty object `{}`.
            If an API key has no assigned privileges, it inherits the owner user's full
            permissions. The snapshot of the owner's permissions is always updated, whether
            you supply the `role_descriptors` parameter. The structure of a role descriptor
            is the same as the request for the create API keys API.
        """
        if ids is None and body is None:
            raise ValueError("Empty value passed for parameter 'ids'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/api_key/_bulk_update"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if ids is not None:
                __body["ids"] = ids
            if expiration is not None:
                __body["expiration"] = expiration
            if metadata is not None:
                __body["metadata"] = metadata
            if role_descriptors is not None:
                __body["role_descriptors"] = role_descriptors
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.bulk_update_api_keys",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("password", "password_hash"),
    )
    async def change_password(
        self,
        *,
        username: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        password: t.Optional[str] = None,
        password_hash: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Change passwords.</p>
          <p>Change the passwords of users in the native realm and built-in users.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-change-password>`_

        :param username: The user whose password you want to change. If you do not specify
            this parameter, the password is changed for the current user.
        :param password: The new password value. Passwords must be at least 6 characters
            long.
        :param password_hash: A hash of the new password value. This must be produced
            using the same hashing algorithm as has been configured for password storage.
            For more details, see the explanation of the `xpack.security.authc.password_hashing.algorithm`
            setting.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        __path_parts: t.Dict[str, str]
        if username not in SKIP_IN_PATH:
            __path_parts = {"username": _quote(username)}
            __path = f'/_security/user/{__path_parts["username"]}/_password'
        else:
            __path_parts = {}
            __path = "/_security/user/_password"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if password is not None:
                __body["password"] = password
            if password_hash is not None:
                __body["password_hash"] = password_hash
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.change_password",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def clear_api_key_cache(
        self,
        *,
        ids: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clear the API key cache.</p>
          <p>Evict a subset of all entries from the API key cache.
          The cache is also automatically cleared on state changes of the security index.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-clear-api-key-cache>`_

        :param ids: Comma-separated list of API key IDs to evict from the API key cache.
            To evict all API keys, use `*`. Does not support other wildcard patterns.
        """
        if ids in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'ids'")
        __path_parts: t.Dict[str, str] = {"ids": _quote(ids)}
        __path = f'/_security/api_key/{__path_parts["ids"]}/_clear_cache'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.clear_api_key_cache",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def clear_cached_privileges(
        self,
        *,
        application: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clear the privileges cache.</p>
          <p>Evict privileges from the native application privilege cache.
          The cache is also automatically cleared for applications that have their privileges updated.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-clear-cached-privileges>`_

        :param application: A comma-separated list of applications. To clear all applications,
            use an asterism (`*`). It does not support other wildcard patterns.
        """
        if application in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'application'")
        __path_parts: t.Dict[str, str] = {"application": _quote(application)}
        __path = f'/_security/privilege/{__path_parts["application"]}/_clear_cache'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.clear_cached_privileges",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def clear_cached_realms(
        self,
        *,
        realms: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        usernames: t.Optional[t.Sequence[str]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clear the user cache.</p>
          <p>Evict users from the user cache.
          You can completely clear the cache or evict specific users.</p>
          <p>User credentials are cached in memory on each node to avoid connecting to a remote authentication service or hitting the disk for every incoming request.
          There are realm settings that you can use to configure the user cache.
          For more information, refer to the documentation about controlling the user cache.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-clear-cached-realms>`_

        :param realms: A comma-separated list of realms. To clear all realms, use an
            asterisk (`*`). It does not support other wildcard patterns.
        :param usernames: A comma-separated list of the users to clear from the cache.
            If you do not specify this parameter, the API evicts all users from the user
            cache.
        """
        if realms in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'realms'")
        __path_parts: t.Dict[str, str] = {"realms": _quote(realms)}
        __path = f'/_security/realm/{__path_parts["realms"]}/_clear_cache'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if usernames is not None:
            __query["usernames"] = usernames
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.clear_cached_realms",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def clear_cached_roles(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clear the roles cache.</p>
          <p>Evict roles from the native role cache.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-clear-cached-roles>`_

        :param name: A comma-separated list of roles to evict from the role cache. To
            evict all roles, use an asterisk (`*`). It does not support other wildcard
            patterns.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_security/role/{__path_parts["name"]}/_clear_cache'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.clear_cached_roles",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def clear_cached_service_tokens(
        self,
        *,
        namespace: str,
        service: str,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clear service account token caches.</p>
          <p>Evict a subset of all entries from the service account token caches.
          Two separate caches exist for service account tokens: one cache for tokens backed by the <code>service_tokens</code> file, and another for tokens backed by the <code>.security</code> index.
          This API clears matching entries from both caches.</p>
          <p>The cache for service account tokens backed by the <code>.security</code> index is cleared automatically on state changes of the security index.
          The cache for tokens backed by the <code>service_tokens</code> file is cleared automatically on file changes.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-clear-cached-service-tokens>`_

        :param namespace: The namespace, which is a top-level grouping of service accounts.
        :param service: The name of the service, which must be unique within its namespace.
        :param name: A comma-separated list of token names to evict from the service
            account token caches. Use a wildcard (`*`) to evict all tokens that belong
            to a service account. It does not support other wildcard patterns.
        """
        if namespace in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'namespace'")
        if service in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'service'")
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {
            "namespace": _quote(namespace),
            "service": _quote(service),
            "name": _quote(name),
        }
        __path = f'/_security/service/{__path_parts["namespace"]}/{__path_parts["service"]}/credential/token/{__path_parts["name"]}/_clear_cache'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.clear_cached_service_tokens",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("expiration", "metadata", "name", "role_descriptors"),
    )
    async def create_api_key(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        expiration: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        name: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        role_descriptors: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an API key.</p>
          <p>Create an API key for access without requiring basic authentication.</p>
          <p>IMPORTANT: If the credential that is used to authenticate this request is an API key, the derived API key cannot have any privileges.
          If you specify privileges, the API returns an error.</p>
          <p>A successful request returns a JSON structure that contains the API key, its unique id, and its name.
          If applicable, it also returns expiration information for the API key in milliseconds.</p>
          <p>NOTE: By default, API keys never expire. You can specify expiration information when you create the API keys.</p>
          <p>The API keys are created by the Elasticsearch API key service, which is automatically enabled.
          To configure or turn off the API key service, refer to API key service setting documentation.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key>`_

        :param expiration: The expiration time for the API key. By default, API keys
            never expire.
        :param metadata: Arbitrary metadata that you want to associate with the API key.
            It supports nested data structure. Within the metadata object, keys beginning
            with `_` are reserved for system usage.
        :param name: A name for the API key.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        :param role_descriptors: An array of role descriptors for this API key. When
            it is not specified or it is an empty array, the API key will have a point
            in time snapshot of permissions of the authenticated user. If you supply
            role descriptors, the resultant permissions are an intersection of API keys
            permissions and the authenticated user's permissions thereby limiting the
            access scope for API keys. The structure of role descriptor is the same as
            the request for the create role API. For more details, refer to the create
            or update roles API. NOTE: Due to the way in which this permission intersection
            is calculated, it is not possible to create an API key that is a child of
            another API key, unless the derived key is created without any privileges.
            In this case, you must explicitly specify a role descriptor with no privileges.
            The derived API key can be used for authentication; it will not have authority
            to call Elasticsearch APIs.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/api_key"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if expiration is not None:
                __body["expiration"] = expiration
            if metadata is not None:
                __body["metadata"] = metadata
            if name is not None:
                __body["name"] = name
            if role_descriptors is not None:
                __body["role_descriptors"] = role_descriptors
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.create_api_key",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("access", "name", "expiration", "metadata"),
    )
    async def create_cross_cluster_api_key(
        self,
        *,
        access: t.Optional[t.Mapping[str, t.Any]] = None,
        name: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        expiration: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a cross-cluster API key.</p>
          <p>Create an API key of the <code>cross_cluster</code> type for the API key based remote cluster access.
          A <code>cross_cluster</code> API key cannot be used to authenticate through the REST interface.</p>
          <p>IMPORTANT: To authenticate this request you must use a credential that is not an API key. Even if you use an API key that has the required privilege, the API returns an error.</p>
          <p>Cross-cluster API keys are created by the Elasticsearch API key service, which is automatically enabled.</p>
          <p>NOTE: Unlike REST API keys, a cross-cluster API key does not capture permissions of the authenticated user. The API key’s effective permission is exactly as specified with the <code>access</code> property.</p>
          <p>A successful request returns a JSON structure that contains the API key, its unique ID, and its name. If applicable, it also returns expiration information for the API key in milliseconds.</p>
          <p>By default, API keys never expire. You can specify expiration information when you create the API keys.</p>
          <p>Cross-cluster API keys can only be updated with the update cross-cluster API key API.
          Attempting to update them with the update REST API key API or the bulk update REST API keys API will result in an error.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-cross-cluster-api-key>`_

        :param access: The access to be granted to this API key. The access is composed
            of permissions for cross-cluster search and cross-cluster replication. At
            least one of them must be specified. NOTE: No explicit privileges should
            be specified for either search or replication access. The creation process
            automatically converts the access specification to a role descriptor which
            has relevant privileges assigned accordingly.
        :param name: Specifies the name for this API key.
        :param expiration: Expiration time for the API key. By default, API keys never
            expire.
        :param metadata: Arbitrary metadata that you want to associate with the API key.
            It supports nested data structure. Within the metadata object, keys beginning
            with `_` are reserved for system usage.
        """
        if access is None and body is None:
            raise ValueError("Empty value passed for parameter 'access'")
        if name is None and body is None:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/cross_cluster/api_key"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if access is not None:
                __body["access"] = access
            if name is not None:
                __body["name"] = name
            if expiration is not None:
                __body["expiration"] = expiration
            if metadata is not None:
                __body["metadata"] = metadata
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.create_cross_cluster_api_key",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def create_service_token(
        self,
        *,
        namespace: str,
        service: str,
        name: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a service account token.</p>
          <p>Create a service accounts token for access without requiring basic authentication.</p>
          <p>NOTE: Service account tokens never expire.
          You must actively delete them if they are no longer needed.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-service-token>`_

        :param namespace: The name of the namespace, which is a top-level grouping of
            service accounts.
        :param service: The name of the service.
        :param name: The name for the service account token. If omitted, a random name
            will be generated. Token names must be at least one and no more than 256
            characters. They can contain alphanumeric characters (a-z, A-Z, 0-9), dashes
            (`-`), and underscores (`_`), but cannot begin with an underscore. NOTE:
            Token names must be unique in the context of the associated service account.
            They must also be globally unique with their fully qualified names, which
            are comprised of the service account principal and token name, such as `<namespace>/<service>/<token-name>`.
        :param refresh: If `true` then refresh the affected shards to make this operation
            visible to search, if `wait_for` (the default) then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if namespace in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'namespace'")
        if service in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'service'")
        __path_parts: t.Dict[str, str]
        if (
            namespace not in SKIP_IN_PATH
            and service not in SKIP_IN_PATH
            and name not in SKIP_IN_PATH
        ):
            __path_parts = {
                "namespace": _quote(namespace),
                "service": _quote(service),
                "name": _quote(name),
            }
            __path = f'/_security/service/{__path_parts["namespace"]}/{__path_parts["service"]}/credential/token/{__path_parts["name"]}'
            __method = "PUT"
        elif namespace not in SKIP_IN_PATH and service not in SKIP_IN_PATH:
            __path_parts = {"namespace": _quote(namespace), "service": _quote(service)}
            __path = f'/_security/service/{__path_parts["namespace"]}/{__path_parts["service"]}/credential/token'
            __method = "POST"
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            __method,
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.create_service_token",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("x509_certificate_chain",),
    )
    async def delegate_pki(
        self,
        *,
        x509_certificate_chain: t.Optional[t.Sequence[str]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delegate PKI authentication.</p>
          <p>This API implements the exchange of an X509Certificate chain for an Elasticsearch access token.
          The certificate chain is validated, according to RFC 5280, by sequentially considering the trust configuration of every installed PKI realm that has <code>delegation.enabled</code> set to <code>true</code>.
          A successfully trusted client certificate is also subject to the validation of the subject distinguished name according to thw <code>username_pattern</code> of the respective realm.</p>
          <p>This API is called by smart and trusted proxies, such as Kibana, which terminate the user's TLS session but still want to authenticate the user by using a PKI realm—-​as if the user connected directly to Elasticsearch.</p>
          <p>IMPORTANT: The association between the subject public key in the target certificate and the corresponding private key is not validated.
          This is part of the TLS authentication process and it is delegated to the proxy that calls this API.
          The proxy is trusted to have performed the TLS authentication and this API translates that authentication into an Elasticsearch access token.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delegate-pki>`_

        :param x509_certificate_chain: The X509Certificate chain, which is represented
            as an ordered string array. Each string in the array is a base64-encoded
            (Section 4 of RFC4648 - not base64url-encoded) of the certificate's DER encoding.
            The first element is the target certificate that contains the subject distinguished
            name that is requesting access. This may be followed by additional certificates;
            each subsequent certificate is used to certify the previous one.
        """
        if x509_certificate_chain is None and body is None:
            raise ValueError(
                "Empty value passed for parameter 'x509_certificate_chain'"
            )
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/delegate_pki"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if x509_certificate_chain is not None:
                __body["x509_certificate_chain"] = x509_certificate_chain
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.delegate_pki",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def delete_privileges(
        self,
        *,
        application: str,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete application privileges.</p>
          <p>To use this API, you must have one of the following privileges:</p>
          <ul>
          <li>The <code>manage_security</code> cluster privilege (or a greater privilege such as <code>all</code>).</li>
          <li>The &quot;Manage Application Privileges&quot; global privilege for the application being referenced in the request.</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delete-privileges>`_

        :param application: The name of the application. Application privileges are always
            associated with exactly one application.
        :param name: The name of the privilege.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if application in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'application'")
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {
            "application": _quote(application),
            "name": _quote(name),
        }
        __path = (
            f'/_security/privilege/{__path_parts["application"]}/{__path_parts["name"]}'
        )
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.delete_privileges",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def delete_role(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete roles.</p>
          <p>Delete roles in the native realm.
          The role management APIs are generally the preferred way to manage roles, rather than using file-based role management.
          The delete roles API cannot remove roles that are defined in roles files.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delete-role>`_

        :param name: The name of the role.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_security/role/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.delete_role",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def delete_role_mapping(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete role mappings.</p>
          <p>Role mappings define which roles are assigned to each user.
          The role mapping APIs are generally the preferred way to manage role mappings rather than using role mapping files.
          The delete role mappings API cannot remove role mappings that are defined in role mapping files.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delete-role-mapping>`_

        :param name: The distinct name that identifies the role mapping. The name is
            used solely as an identifier to facilitate interaction via the API; it does
            not affect the behavior of the mapping in any way.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_security/role_mapping/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.delete_role_mapping",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def delete_service_token(
        self,
        *,
        namespace: str,
        service: str,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete service account tokens.</p>
          <p>Delete service account tokens for a service in a specified namespace.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delete-service-token>`_

        :param namespace: The namespace, which is a top-level grouping of service accounts.
        :param service: The service name.
        :param name: The name of the service account token.
        :param refresh: If `true` then refresh the affected shards to make this operation
            visible to search, if `wait_for` (the default) then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if namespace in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'namespace'")
        if service in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'service'")
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {
            "namespace": _quote(namespace),
            "service": _quote(service),
            "name": _quote(name),
        }
        __path = f'/_security/service/{__path_parts["namespace"]}/{__path_parts["service"]}/credential/token/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.delete_service_token",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def delete_user(
        self,
        *,
        username: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete users.</p>
          <p>Delete users from the native realm.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-delete-user>`_

        :param username: An identifier for the user.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if username in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'username'")
        __path_parts: t.Dict[str, str] = {"username": _quote(username)}
        __path = f'/_security/user/{__path_parts["username"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.delete_user",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def disable_user(
        self,
        *,
        username: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Disable users.</p>
          <p>Disable users in the native realm.
          By default, when you create users, they are enabled.
          You can use this API to revoke a user's access to Elasticsearch.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-disable-user>`_

        :param username: An identifier for the user.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if username in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'username'")
        __path_parts: t.Dict[str, str] = {"username": _quote(username)}
        __path = f'/_security/user/{__path_parts["username"]}/_disable'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.disable_user",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def disable_user_profile(
        self,
        *,
        uid: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Disable a user profile.</p>
          <p>Disable user profiles so that they are not visible in user profile searches.</p>
          <p>NOTE: The user profile feature is designed only for use by Kibana and Elastic's Observability, Enterprise Search, and Elastic Security solutions.
          Individual users and external applications should not call this API directly.
          Elastic reserves the right to change or remove this feature in future releases without prior notice.</p>
          <p>When you activate a user profile, its automatically enabled and visible in user profile searches. You can use the disable user profile API to disable a user profile so it’s not visible in these searches.
          To re-enable a disabled user profile, use the enable user profile API .</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-disable-user-profile>`_

        :param uid: Unique identifier for the user profile.
        :param refresh: If 'true', Elasticsearch refreshes the affected shards to make
            this operation visible to search. If 'wait_for', it waits for a refresh to
            make this operation visible to search. If 'false', it does nothing with refreshes.
        """
        if uid in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'uid'")
        __path_parts: t.Dict[str, str] = {"uid": _quote(uid)}
        __path = f'/_security/profile/{__path_parts["uid"]}/_disable'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.disable_user_profile",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def enable_user(
        self,
        *,
        username: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Enable users.</p>
          <p>Enable users in the native realm.
          By default, when you create users, they are enabled.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-enable-user>`_

        :param username: An identifier for the user.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if username in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'username'")
        __path_parts: t.Dict[str, str] = {"username": _quote(username)}
        __path = f'/_security/user/{__path_parts["username"]}/_enable'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.enable_user",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def enable_user_profile(
        self,
        *,
        uid: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Enable a user profile.</p>
          <p>Enable user profiles to make them visible in user profile searches.</p>
          <p>NOTE: The user profile feature is designed only for use by Kibana and Elastic's Observability, Enterprise Search, and Elastic Security solutions.
          Individual users and external applications should not call this API directly.
          Elastic reserves the right to change or remove this feature in future releases without prior notice.</p>
          <p>When you activate a user profile, it's automatically enabled and visible in user profile searches.
          If you later disable the user profile, you can use the enable user profile API to make the profile visible in these searches again.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-enable-user-profile>`_

        :param uid: A unique identifier for the user profile.
        :param refresh: If 'true', Elasticsearch refreshes the affected shards to make
            this operation visible to search. If 'wait_for', it waits for a refresh to
            make this operation visible to search. If 'false', nothing is done with refreshes.
        """
        if uid in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'uid'")
        __path_parts: t.Dict[str, str] = {"uid": _quote(uid)}
        __path = f'/_security/profile/{__path_parts["uid"]}/_enable'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.enable_user_profile",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def enroll_kibana(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Enroll Kibana.</p>
          <p>Enable a Kibana instance to configure itself for communication with a secured Elasticsearch cluster.</p>
          <p>NOTE: This API is currently intended for internal use only by Kibana.
          Kibana uses this API internally to configure itself for communications with an Elasticsearch cluster that already has security features enabled.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-enroll-kibana>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/enroll/kibana"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.enroll_kibana",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def enroll_node(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Enroll a node.</p>
          <p>Enroll a new node to allow it to join an existing cluster with security features enabled.</p>
          <p>The response contains all the necessary information for the joining node to bootstrap discovery and security related settings so that it can successfully join the cluster.
          The response contains key and certificate material that allows the caller to generate valid signed certificates for the HTTP layer of all nodes in the cluster.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-enroll-node>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/enroll/node"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.enroll_node",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_api_key(
        self,
        *,
        active_only: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        id: t.Optional[str] = None,
        name: t.Optional[str] = None,
        owner: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        realm_name: t.Optional[str] = None,
        username: t.Optional[str] = None,
        with_limited_by: t.Optional[bool] = None,
        with_profile_uid: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get API key information.</p>
          <p>Retrieves information for one or more API keys.
          NOTE: If you have only the <code>manage_own_api_key</code> privilege, this API returns only the API keys that you own.
          If you have <code>read_security</code>, <code>manage_api_key</code> or greater privileges (including <code>manage_security</code>), this API returns all API keys regardless of ownership.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-api-key>`_

        :param active_only: A boolean flag that can be used to query API keys that are
            currently active. An API key is considered active if it is neither invalidated,
            nor expired at query time. You can specify this together with other parameters
            such as `owner` or `name`. If `active_only` is false, the response will include
            both active and inactive (expired or invalidated) keys.
        :param id: An API key id. This parameter cannot be used with any of `name`, `realm_name`
            or `username`.
        :param name: An API key name. This parameter cannot be used with any of `id`,
            `realm_name` or `username`. It supports prefix search with wildcard.
        :param owner: A boolean flag that can be used to query API keys owned by the
            currently authenticated user. The `realm_name` or `username` parameters cannot
            be specified when this parameter is set to `true` as they are assumed to
            be the currently authenticated ones.
        :param realm_name: The name of an authentication realm. This parameter cannot
            be used with either `id` or `name` or when `owner` flag is set to `true`.
        :param username: The username of a user. This parameter cannot be used with either
            `id` or `name` or when `owner` flag is set to `true`.
        :param with_limited_by: Return the snapshot of the owner user's role descriptors
            associated with the API key. An API key's actual permission is the intersection
            of its assigned role descriptors and the owner user's role descriptors.
        :param with_profile_uid: Determines whether to also retrieve the profile uid,
            for the API key owner principal, if it exists.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/api_key"
        __query: t.Dict[str, t.Any] = {}
        if active_only is not None:
            __query["active_only"] = active_only
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if id is not None:
            __query["id"] = id
        if name is not None:
            __query["name"] = name
        if owner is not None:
            __query["owner"] = owner
        if pretty is not None:
            __query["pretty"] = pretty
        if realm_name is not None:
            __query["realm_name"] = realm_name
        if username is not None:
            __query["username"] = username
        if with_limited_by is not None:
            __query["with_limited_by"] = with_limited_by
        if with_profile_uid is not None:
            __query["with_profile_uid"] = with_profile_uid
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_api_key",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_builtin_privileges(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get builtin privileges.</p>
          <p>Get the list of cluster privileges and index privileges that are available in this version of Elasticsearch.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-builtin-privileges>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/privilege/_builtin"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_builtin_privileges",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_privileges(
        self,
        *,
        application: t.Optional[str] = None,
        name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get application privileges.</p>
          <p>To use this API, you must have one of the following privileges:</p>
          <ul>
          <li>The <code>read_security</code> cluster privilege (or a greater privilege such as <code>manage_security</code> or <code>all</code>).</li>
          <li>The &quot;Manage Application Privileges&quot; global privilege for the application being referenced in the request.</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-privileges>`_

        :param application: The name of the application. Application privileges are always
            associated with exactly one application. If you do not specify this parameter,
            the API returns information about all privileges for all applications.
        :param name: The name of the privilege. If you do not specify this parameter,
            the API returns information about all privileges for the requested application.
        """
        __path_parts: t.Dict[str, str]
        if application not in SKIP_IN_PATH and name not in SKIP_IN_PATH:
            __path_parts = {"application": _quote(application), "name": _quote(name)}
            __path = f'/_security/privilege/{__path_parts["application"]}/{__path_parts["name"]}'
        elif application not in SKIP_IN_PATH:
            __path_parts = {"application": _quote(application)}
            __path = f'/_security/privilege/{__path_parts["application"]}'
        else:
            __path_parts = {}
            __path = "/_security/privilege"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_privileges",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_role(
        self,
        *,
        name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get roles.</p>
          <p>Get roles in the native realm.
          The role management APIs are generally the preferred way to manage roles, rather than using file-based role management.
          The get roles API cannot retrieve roles that are defined in roles files.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-role>`_

        :param name: The name of the role. You can specify multiple roles as a comma-separated
            list. If you do not specify this parameter, the API returns information about
            all roles.
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_security/role/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_security/role"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_role",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_role_mapping(
        self,
        *,
        name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get role mappings.</p>
          <p>Role mappings define which roles are assigned to each user.
          The role mapping APIs are generally the preferred way to manage role mappings rather than using role mapping files.
          The get role mappings API cannot retrieve role mappings that are defined in role mapping files.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-role-mapping>`_

        :param name: The distinct name that identifies the role mapping. The name is
            used solely as an identifier to facilitate interaction via the API; it does
            not affect the behavior of the mapping in any way. You can specify multiple
            mapping names as a comma-separated list. If you do not specify this parameter,
            the API returns information about all role mappings.
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_security/role_mapping/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_security/role_mapping"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_role_mapping",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_service_accounts(
        self,
        *,
        namespace: t.Optional[str] = None,
        service: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get service accounts.</p>
          <p>Get a list of service accounts that match the provided path parameters.</p>
          <p>NOTE: Currently, only the <code>elastic/fleet-server</code> service account is available.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-service-accounts>`_

        :param namespace: The name of the namespace. Omit this parameter to retrieve
            information about all service accounts. If you omit this parameter, you must
            also omit the `service` parameter.
        :param service: The service name. Omit this parameter to retrieve information
            about all service accounts that belong to the specified `namespace`.
        """
        __path_parts: t.Dict[str, str]
        if namespace not in SKIP_IN_PATH and service not in SKIP_IN_PATH:
            __path_parts = {"namespace": _quote(namespace), "service": _quote(service)}
            __path = f'/_security/service/{__path_parts["namespace"]}/{__path_parts["service"]}'
        elif namespace not in SKIP_IN_PATH:
            __path_parts = {"namespace": _quote(namespace)}
            __path = f'/_security/service/{__path_parts["namespace"]}'
        else:
            __path_parts = {}
            __path = "/_security/service"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_service_accounts",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_service_credentials(
        self,
        *,
        namespace: str,
        service: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get service account credentials.</p>
          <p>To use this API, you must have at least the <code>read_security</code> cluster privilege (or a greater privilege such as <code>manage_service_account</code> or <code>manage_security</code>).</p>
          <p>The response includes service account tokens that were created with the create service account tokens API as well as file-backed tokens from all nodes of the cluster.</p>
          <p>NOTE: For tokens backed by the <code>service_tokens</code> file, the API collects them from all nodes of the cluster.
          Tokens with the same name from different nodes are assumed to be the same token and are only counted once towards the total number of service tokens.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-service-credentials>`_

        :param namespace: The name of the namespace.
        :param service: The service name.
        """
        if namespace in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'namespace'")
        if service in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'service'")
        __path_parts: t.Dict[str, str] = {
            "namespace": _quote(namespace),
            "service": _quote(service),
        }
        __path = f'/_security/service/{__path_parts["namespace"]}/{__path_parts["service"]}/credential'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_service_credentials",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_settings(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get security index settings.</p>
          <p>Get the user-configurable settings for the security internal index (<code>.security</code> and associated indices).
          Only a subset of the index settings — those that are user-configurable—will be shown.
          This includes:</p>
          <ul>
          <li><code>index.auto_expand_replicas</code></li>
          <li><code>index.number_of_replicas</code></li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-settings>`_

        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/settings"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_settings",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "grant_type",
            "kerberos_ticket",
            "password",
            "refresh_token",
            "scope",
            "username",
        ),
    )
    async def get_token(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        grant_type: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "_kerberos", "client_credentials", "password", "refresh_token"
                ],
            ]
        ] = None,
        human: t.Optional[bool] = None,
        kerberos_ticket: t.Optional[str] = None,
        password: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh_token: t.Optional[str] = None,
        scope: t.Optional[str] = None,
        username: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a token.</p>
          <p>Create a bearer token for access without requiring basic authentication.
          The tokens are created by the Elasticsearch Token Service, which is automatically enabled when you configure TLS on the HTTP interface.
          Alternatively, you can explicitly enable the <code>xpack.security.authc.token.enabled</code> setting.
          When you are running in production mode, a bootstrap check prevents you from enabling the token service unless you also enable TLS on the HTTP interface.</p>
          <p>The get token API takes the same parameters as a typical OAuth 2.0 token API except for the use of a JSON request body.</p>
          <p>A successful get token API call returns a JSON structure that contains the access token, the amount of time (seconds) that the token expires in, the type, and the scope if available.</p>
          <p>The tokens returned by the get token API have a finite period of time for which they are valid and after that time period, they can no longer be used.
          That time period is defined by the <code>xpack.security.authc.token.timeout</code> setting.
          If you want to invalidate a token immediately, you can do so by using the invalidate token API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-token>`_

        :param grant_type: The type of grant. Supported grant types are: `password`,
            `_kerberos`, `client_credentials`, and `refresh_token`.
        :param kerberos_ticket: The base64 encoded kerberos ticket. If you specify the
            `_kerberos` grant type, this parameter is required. This parameter is not
            valid with any other supported grant type.
        :param password: The user's password. If you specify the `password` grant type,
            this parameter is required. This parameter is not valid with any other supported
            grant type.
        :param refresh_token: The string that was returned when you created the token,
            which enables you to extend its life. If you specify the `refresh_token`
            grant type, this parameter is required. This parameter is not valid with
            any other supported grant type.
        :param scope: The scope of the token. Currently tokens are only issued for a
            scope of FULL regardless of the value sent with the request.
        :param username: The username that identifies the user. If you specify the `password`
            grant type, this parameter is required. This parameter is not valid with
            any other supported grant type.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/oauth2/token"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if grant_type is not None:
                __body["grant_type"] = grant_type
            if kerberos_ticket is not None:
                __body["kerberos_ticket"] = kerberos_ticket
            if password is not None:
                __body["password"] = password
            if refresh_token is not None:
                __body["refresh_token"] = refresh_token
            if scope is not None:
                __body["scope"] = scope
            if username is not None:
                __body["username"] = username
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.get_token",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_user(
        self,
        *,
        username: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        with_profile_uid: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get users.</p>
          <p>Get information about users in the native realm and built-in users.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-user>`_

        :param username: An identifier for the user. You can specify multiple usernames
            as a comma-separated list. If you omit this parameter, the API retrieves
            information about all users.
        :param with_profile_uid: Determines whether to retrieve the user profile UID,
            if it exists, for the users.
        """
        __path_parts: t.Dict[str, str]
        if username not in SKIP_IN_PATH:
            __path_parts = {"username": _quote(username)}
            __path = f'/_security/user/{__path_parts["username"]}'
        else:
            __path_parts = {}
            __path = "/_security/user"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if with_profile_uid is not None:
            __query["with_profile_uid"] = with_profile_uid
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_user",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_user_privileges(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get user privileges.</p>
          <p>Get the security privileges for the logged in user.
          All users can use this API, but only to determine their own privileges.
          To check the privileges of other users, you must use the run as feature.
          To check whether a user has a specific list of privileges, use the has privileges API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-user-privileges>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/user/_privileges"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_user_privileges",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_user_profile(
        self,
        *,
        uid: t.Union[str, t.Sequence[str]],
        data: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a user profile.</p>
          <p>Get a user's profile using the unique profile ID.</p>
          <p>NOTE: The user profile feature is designed only for use by Kibana and Elastic's Observability, Enterprise Search, and Elastic Security solutions.
          Individual users and external applications should not call this API directly.
          Elastic reserves the right to change or remove this feature in future releases without prior notice.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-get-user-profile>`_

        :param uid: A unique identifier for the user profile.
        :param data: A comma-separated list of filters for the `data` field of the profile
            document. To return all content use `data=*`. To return a subset of content
            use `data=<key>` to retrieve content nested under the specified `<key>`.
            By default returns no `data` content.
        """
        if uid in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'uid'")
        __path_parts: t.Dict[str, str] = {"uid": _quote(uid)}
        __path = f'/_security/profile/{__path_parts["uid"]}'
        __query: t.Dict[str, t.Any] = {}
        if data is not None:
            __query["data"] = data
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.get_user_profile",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "api_key",
            "grant_type",
            "access_token",
            "password",
            "run_as",
            "username",
        ),
        ignore_deprecated_options={"api_key"},
    )
    async def grant_api_key(
        self,
        *,
        api_key: t.Optional[t.Mapping[str, t.Any]] = None,
        grant_type: t.Optional[
            t.Union[str, t.Literal["access_token", "password"]]
        ] = None,
        access_token: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        password: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        run_as: t.Optional[str] = None,
        username: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Grant an API key.</p>
          <p>Create an API key on behalf of another user.
          This API is similar to the create API keys API, however it creates the API key for a user that is different than the user that runs the API.
          The caller must have authentication credentials for the user on whose behalf the API key will be created.
          It is not possible to use this API to create an API key without that user's credentials.
          The supported user authentication credential types are:</p>
          <ul>
          <li>username and password</li>
          <li>Elasticsearch access tokens</li>
          <li>JWTs</li>
          </ul>
          <p>The user, for whom the authentication credentials is provided, can optionally &quot;run as&quot; (impersonate) another user.
          In this case, the API key will be created on behalf of the impersonated user.</p>
          <p>This API is intended be used by applications that need to create and manage API keys for end users, but cannot guarantee that those users have permission to create API keys on their own behalf.
          The API keys are created by the Elasticsearch API key service, which is automatically enabled.</p>
          <p>A successful grant API key API call returns a JSON structure that contains the API key, its unique id, and its name.
          If applicable, it also returns expiration information for the API key in milliseconds.</p>
          <p>By default, API keys never expire. You can specify expiration information when you create the API keys.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-grant-api-key>`_

        :param api_key: The API key.
        :param grant_type: The type of grant. Supported grant types are: `access_token`,
            `password`.
        :param access_token: The user's access token. If you specify the `access_token`
            grant type, this parameter is required. It is not valid with other grant
            types.
        :param password: The user's password. If you specify the `password` grant type,
            this parameter is required. It is not valid with other grant types.
        :param refresh: If 'true', Elasticsearch refreshes the affected shards to make
            this operation visible to search. If 'wait_for', it waits for a refresh to
            make this operation visible to search. If 'false', nothing is done with refreshes.
        :param run_as: The name of the user to be impersonated.
        :param username: The user name that identifies the user. If you specify the `password`
            grant type, this parameter is required. It is not valid with other grant
            types.
        """
        if api_key is None and body is None:
            raise ValueError("Empty value passed for parameter 'api_key'")
        if grant_type is None and body is None:
            raise ValueError("Empty value passed for parameter 'grant_type'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/api_key/grant"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if api_key is not None:
                __body["api_key"] = api_key
            if grant_type is not None:
                __body["grant_type"] = grant_type
            if access_token is not None:
                __body["access_token"] = access_token
            if password is not None:
                __body["password"] = password
            if run_as is not None:
                __body["run_as"] = run_as
            if username is not None:
                __body["username"] = username
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.grant_api_key",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("application", "cluster", "index"),
    )
    async def has_privileges(
        self,
        *,
        user: t.Optional[str] = None,
        application: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        cluster: t.Optional[
            t.Sequence[
                t.Union[
                    str,
                    t.Literal[
                        "all",
                        "cancel_task",
                        "create_snapshot",
                        "cross_cluster_replication",
                        "cross_cluster_search",
                        "delegate_pki",
                        "grant_api_key",
                        "manage",
                        "manage_api_key",
                        "manage_autoscaling",
                        "manage_behavioral_analytics",
                        "manage_ccr",
                        "manage_data_frame_transforms",
                        "manage_data_stream_global_retention",
                        "manage_enrich",
                        "manage_esql",
                        "manage_ilm",
                        "manage_index_templates",
                        "manage_inference",
                        "manage_ingest_pipelines",
                        "manage_logstash_pipelines",
                        "manage_ml",
                        "manage_oidc",
                        "manage_own_api_key",
                        "manage_pipeline",
                        "manage_rollup",
                        "manage_saml",
                        "manage_search_application",
                        "manage_search_query_rules",
                        "manage_search_synonyms",
                        "manage_security",
                        "manage_service_account",
                        "manage_slm",
                        "manage_token",
                        "manage_transform",
                        "manage_user_profile",
                        "manage_watcher",
                        "monitor",
                        "monitor_data_frame_transforms",
                        "monitor_data_stream_global_retention",
                        "monitor_enrich",
                        "monitor_esql",
                        "monitor_inference",
                        "monitor_ml",
                        "monitor_rollup",
                        "monitor_snapshot",
                        "monitor_stats",
                        "monitor_text_structure",
                        "monitor_transform",
                        "monitor_watcher",
                        "none",
                        "post_behavioral_analytics_event",
                        "read_ccr",
                        "read_fleet_secrets",
                        "read_ilm",
                        "read_pipeline",
                        "read_security",
                        "read_slm",
                        "transport_client",
                        "write_connector_secrets",
                        "write_fleet_secrets",
                    ],
                ]
            ]
        ] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        index: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Check user privileges.</p>
          <p>Determine whether the specified user has a specified list of privileges.
          All users can use this API, but only to determine their own privileges.
          To check the privileges of other users, you must use the run as feature.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-has-privileges>`_

        :param user: Username
        :param application:
        :param cluster: A list of the cluster privileges that you want to check.
        :param index:
        """
        __path_parts: t.Dict[str, str]
        if user not in SKIP_IN_PATH:
            __path_parts = {"user": _quote(user)}
            __path = f'/_security/user/{__path_parts["user"]}/_has_privileges'
        else:
            __path_parts = {}
            __path = "/_security/user/_has_privileges"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if application is not None:
                __body["application"] = application
            if cluster is not None:
                __body["cluster"] = cluster
            if index is not None:
                __body["index"] = index
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.has_privileges",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("privileges", "uids"),
    )
    async def has_privileges_user_profile(
        self,
        *,
        privileges: t.Optional[t.Mapping[str, t.Any]] = None,
        uids: t.Optional[t.Sequence[str]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Check user profile privileges.</p>
          <p>Determine whether the users associated with the specified user profile IDs have all the requested privileges.</p>
          <p>NOTE: The user profile feature is designed only for use by Kibana and Elastic's Observability, Enterprise Search, and Elastic Security solutions. Individual users and external applications should not call this API directly.
          Elastic reserves the right to change or remove this feature in future releases without prior notice.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-has-privileges-user-profile>`_

        :param privileges: An object containing all the privileges to be checked.
        :param uids: A list of profile IDs. The privileges are checked for associated
            users of the profiles.
        """
        if privileges is None and body is None:
            raise ValueError("Empty value passed for parameter 'privileges'")
        if uids is None and body is None:
            raise ValueError("Empty value passed for parameter 'uids'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/profile/_has_privileges"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if privileges is not None:
                __body["privileges"] = privileges
            if uids is not None:
                __body["uids"] = uids
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.has_privileges_user_profile",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("id", "ids", "name", "owner", "realm_name", "username"),
    )
    async def invalidate_api_key(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        id: t.Optional[str] = None,
        ids: t.Optional[t.Sequence[str]] = None,
        name: t.Optional[str] = None,
        owner: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        realm_name: t.Optional[str] = None,
        username: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Invalidate API keys.</p>
          <p>This API invalidates API keys created by the create API key or grant API key APIs.
          Invalidated API keys fail authentication, but they can still be viewed using the get API key information and query API key information APIs, for at least the configured retention period, until they are automatically deleted.</p>
          <p>To use this API, you must have at least the <code>manage_security</code>, <code>manage_api_key</code>, or <code>manage_own_api_key</code> cluster privileges.
          The <code>manage_security</code> privilege allows deleting any API key, including both REST and cross cluster API keys.
          The <code>manage_api_key</code> privilege allows deleting any REST API key, but not cross cluster API keys.
          The <code>manage_own_api_key</code> only allows deleting REST API keys that are owned by the user.
          In addition, with the <code>manage_own_api_key</code> privilege, an invalidation request must be issued in one of the three formats:</p>
          <ul>
          <li>Set the parameter <code>owner=true</code>.</li>
          <li>Or, set both <code>username</code> and <code>realm_name</code> to match the user's identity.</li>
          <li>Or, if the request is issued by an API key, that is to say an API key invalidates itself, specify its ID in the <code>ids</code> field.</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-invalidate-api-key>`_

        :param id:
        :param ids: A list of API key ids. This parameter cannot be used with any of
            `name`, `realm_name`, or `username`.
        :param name: An API key name. This parameter cannot be used with any of `ids`,
            `realm_name` or `username`.
        :param owner: Query API keys owned by the currently authenticated user. The `realm_name`
            or `username` parameters cannot be specified when this parameter is set to
            `true` as they are assumed to be the currently authenticated ones. NOTE:
            At least one of `ids`, `name`, `username`, and `realm_name` must be specified
            if `owner` is `false`.
        :param realm_name: The name of an authentication realm. This parameter cannot
            be used with either `ids` or `name`, or when `owner` flag is set to `true`.
        :param username: The username of a user. This parameter cannot be used with either
            `ids` or `name` or when `owner` flag is set to `true`.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/api_key"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if id is not None:
                __body["id"] = id
            if ids is not None:
                __body["ids"] = ids
            if name is not None:
                __body["name"] = name
            if owner is not None:
                __body["owner"] = owner
            if realm_name is not None:
                __body["realm_name"] = realm_name
            if username is not None:
                __body["username"] = username
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.invalidate_api_key",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("realm_name", "refresh_token", "token", "username"),
    )
    async def invalidate_token(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        realm_name: t.Optional[str] = None,
        refresh_token: t.Optional[str] = None,
        token: t.Optional[str] = None,
        username: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Invalidate a token.</p>
          <p>The access tokens returned by the get token API have a finite period of time for which they are valid.
          After that time period, they can no longer be used.
          The time period is defined by the <code>xpack.security.authc.token.timeout</code> setting.</p>
          <p>The refresh tokens returned by the get token API are only valid for 24 hours.
          They can also be used exactly once.
          If you want to invalidate one or more access or refresh tokens immediately, use this invalidate token API.</p>
          <p>NOTE: While all parameters are optional, at least one of them is required.
          More specifically, either one of <code>token</code> or <code>refresh_token</code> parameters is required.
          If none of these two are specified, then <code>realm_name</code> and/or <code>username</code> need to be specified.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-invalidate-token>`_

        :param realm_name: The name of an authentication realm. This parameter cannot
            be used with either `refresh_token` or `token`.
        :param refresh_token: A refresh token. This parameter cannot be used if any of
            `refresh_token`, `realm_name`, or `username` are used.
        :param token: An access token. This parameter cannot be used if any of `refresh_token`,
            `realm_name`, or `username` are used.
        :param username: The username of a user. This parameter cannot be used with either
            `refresh_token` or `token`.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/oauth2/token"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if realm_name is not None:
                __body["realm_name"] = realm_name
            if refresh_token is not None:
                __body["refresh_token"] = refresh_token
            if token is not None:
                __body["token"] = token
            if username is not None:
                __body["username"] = username
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.invalidate_token",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("nonce", "redirect_uri", "state", "realm"),
    )
    async def oidc_authenticate(
        self,
        *,
        nonce: t.Optional[str] = None,
        redirect_uri: t.Optional[str] = None,
        state: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        realm: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Authenticate OpenID Connect.</p>
          <p>Exchange an OpenID Connect authentication response message for an Elasticsearch internal access token and refresh token that can be subsequently used for authentication.</p>
          <p>Elasticsearch exposes all the necessary OpenID Connect related functionality with the OpenID Connect APIs.
          These APIs are used internally by Kibana in order to provide OpenID Connect based authentication, but can also be used by other, custom web applications or other clients.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-oidc-authenticate>`_

        :param nonce: Associate a client session with an ID token and mitigate replay
            attacks. This value needs to be the same as the one that was provided to
            the `/_security/oidc/prepare` API or the one that was generated by Elasticsearch
            and included in the response to that call.
        :param redirect_uri: The URL to which the OpenID Connect Provider redirected
            the User Agent in response to an authentication request after a successful
            authentication. This URL must be provided as-is (URL encoded), taken from
            the body of the response or as the value of a location header in the response
            from the OpenID Connect Provider.
        :param state: Maintain state between the authentication request and the response.
            This value needs to be the same as the one that was provided to the `/_security/oidc/prepare`
            API or the one that was generated by Elasticsearch and included in the response
            to that call.
        :param realm: The name of the OpenID Connect realm. This property is useful in
            cases where multiple realms are defined.
        """
        if nonce is None and body is None:
            raise ValueError("Empty value passed for parameter 'nonce'")
        if redirect_uri is None and body is None:
            raise ValueError("Empty value passed for parameter 'redirect_uri'")
        if state is None and body is None:
            raise ValueError("Empty value passed for parameter 'state'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/oidc/authenticate"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if nonce is not None:
                __body["nonce"] = nonce
            if redirect_uri is not None:
                __body["redirect_uri"] = redirect_uri
            if state is not None:
                __body["state"] = state
            if realm is not None:
                __body["realm"] = realm
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.oidc_authenticate",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("token", "refresh_token"),
    )
    async def oidc_logout(
        self,
        *,
        token: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh_token: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Logout of OpenID Connect.</p>
          <p>Invalidate an access token and a refresh token that were generated as a response to the <code>/_security/oidc/authenticate</code> API.</p>
          <p>If the OpenID Connect authentication realm in Elasticsearch is accordingly configured, the response to this call will contain a URI pointing to the end session endpoint of the OpenID Connect Provider in order to perform single logout.</p>
          <p>Elasticsearch exposes all the necessary OpenID Connect related functionality with the OpenID Connect APIs.
          These APIs are used internally by Kibana in order to provide OpenID Connect based authentication, but can also be used by other, custom web applications or other clients.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-oidc-logout>`_

        :param token: The access token to be invalidated.
        :param refresh_token: The refresh token to be invalidated.
        """
        if token is None and body is None:
            raise ValueError("Empty value passed for parameter 'token'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/oidc/logout"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if token is not None:
                __body["token"] = token
            if refresh_token is not None:
                __body["refresh_token"] = refresh_token
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.oidc_logout",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("iss", "login_hint", "nonce", "realm", "state"),
    )
    async def oidc_prepare_authentication(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        iss: t.Optional[str] = None,
        login_hint: t.Optional[str] = None,
        nonce: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        realm: t.Optional[str] = None,
        state: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Prepare OpenID connect authentication.</p>
          <p>Create an oAuth 2.0 authentication request as a URL string based on the configuration of the OpenID Connect authentication realm in Elasticsearch.</p>
          <p>The response of this API is a URL pointing to the Authorization Endpoint of the configured OpenID Connect Provider, which can be used to redirect the browser of the user in order to continue the authentication process.</p>
          <p>Elasticsearch exposes all the necessary OpenID Connect related functionality with the OpenID Connect APIs.
          These APIs are used internally by Kibana in order to provide OpenID Connect based authentication, but can also be used by other, custom web applications or other clients.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-oidc-prepare-authentication>`_

        :param iss: In the case of a third party initiated single sign on, this is the
            issuer identifier for the OP that the RP is to send the authentication request
            to. It cannot be specified when *realm* is specified. One of *realm* or *iss*
            is required.
        :param login_hint: In the case of a third party initiated single sign on, it
            is a string value that is included in the authentication request as the *login_hint*
            parameter. This parameter is not valid when *realm* is specified.
        :param nonce: The value used to associate a client session with an ID token and
            to mitigate replay attacks. If the caller of the API does not provide a value,
            Elasticsearch will generate one with sufficient entropy and return it in
            the response.
        :param realm: The name of the OpenID Connect realm in Elasticsearch the configuration
            of which should be used in order to generate the authentication request.
            It cannot be specified when *iss* is specified. One of *realm* or *iss* is
            required.
        :param state: The value used to maintain state between the authentication request
            and the response, typically used as a Cross-Site Request Forgery mitigation.
            If the caller of the API does not provide a value, Elasticsearch will generate
            one with sufficient entropy and return it in the response.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/oidc/prepare"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if iss is not None:
                __body["iss"] = iss
            if login_hint is not None:
                __body["login_hint"] = login_hint
            if nonce is not None:
                __body["nonce"] = nonce
            if realm is not None:
                __body["realm"] = realm
            if state is not None:
                __body["state"] = state
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.oidc_prepare_authentication",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="privileges",
    )
    async def put_privileges(
        self,
        *,
        privileges: t.Optional[
            t.Mapping[str, t.Mapping[str, t.Mapping[str, t.Any]]]
        ] = None,
        body: t.Optional[t.Mapping[str, t.Mapping[str, t.Mapping[str, t.Any]]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update application privileges.</p>
          <p>To use this API, you must have one of the following privileges:</p>
          <ul>
          <li>The <code>manage_security</code> cluster privilege (or a greater privilege such as <code>all</code>).</li>
          <li>The &quot;Manage Application Privileges&quot; global privilege for the application being referenced in the request.</li>
          </ul>
          <p>Application names are formed from a prefix, with an optional suffix that conform to the following rules:</p>
          <ul>
          <li>The prefix must begin with a lowercase ASCII letter.</li>
          <li>The prefix must contain only ASCII letters or digits.</li>
          <li>The prefix must be at least 3 characters long.</li>
          <li>If the suffix exists, it must begin with either a dash <code>-</code> or <code>_</code>.</li>
          <li>The suffix cannot contain any of the following characters: <code>\\</code>, <code>/</code>, <code>*</code>, <code>?</code>, <code>&quot;</code>, <code>&lt;</code>, <code>&gt;</code>, <code>|</code>, <code>,</code>, <code>*</code>.</li>
          <li>No part of the name can contain whitespace.</li>
          </ul>
          <p>Privilege names must begin with a lowercase ASCII letter and must contain only ASCII letters and digits along with the characters <code>_</code>, <code>-</code>, and <code>.</code>.</p>
          <p>Action names can contain any number of printable ASCII characters and must contain at least one of the following characters: <code>/</code>, <code>*</code>, <code>:</code>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-privileges>`_

        :param privileges:
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        """
        if privileges is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'privileges' and 'body', one of them should be set."
            )
        elif privileges is not None and body is not None:
            raise ValueError("Cannot set both 'privileges' and 'body'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/privilege"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __body = privileges if privileges is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.put_privileges",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "applications",
            "cluster",
            "description",
            "global_",
            "indices",
            "metadata",
            "remote_cluster",
            "remote_indices",
            "run_as",
            "transient_metadata",
        ),
        parameter_aliases={"global": "global_"},
    )
    async def put_role(
        self,
        *,
        name: str,
        applications: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        cluster: t.Optional[
            t.Sequence[
                t.Union[
                    str,
                    t.Literal[
                        "all",
                        "cancel_task",
                        "create_snapshot",
                        "cross_cluster_replication",
                        "cross_cluster_search",
                        "delegate_pki",
                        "grant_api_key",
                        "manage",
                        "manage_api_key",
                        "manage_autoscaling",
                        "manage_behavioral_analytics",
                        "manage_ccr",
                        "manage_data_frame_transforms",
                        "manage_data_stream_global_retention",
                        "manage_enrich",
                        "manage_esql",
                        "manage_ilm",
                        "manage_index_templates",
                        "manage_inference",
                        "manage_ingest_pipelines",
                        "manage_logstash_pipelines",
                        "manage_ml",
                        "manage_oidc",
                        "manage_own_api_key",
                        "manage_pipeline",
                        "manage_rollup",
                        "manage_saml",
                        "manage_search_application",
                        "manage_search_query_rules",
                        "manage_search_synonyms",
                        "manage_security",
                        "manage_service_account",
                        "manage_slm",
                        "manage_token",
                        "manage_transform",
                        "manage_user_profile",
                        "manage_watcher",
                        "monitor",
                        "monitor_data_frame_transforms",
                        "monitor_data_stream_global_retention",
                        "monitor_enrich",
                        "monitor_esql",
                        "monitor_inference",
                        "monitor_ml",
                        "monitor_rollup",
                        "monitor_snapshot",
                        "monitor_stats",
                        "monitor_text_structure",
                        "monitor_transform",
                        "monitor_watcher",
                        "none",
                        "post_behavioral_analytics_event",
                        "read_ccr",
                        "read_fleet_secrets",
                        "read_ilm",
                        "read_pipeline",
                        "read_security",
                        "read_slm",
                        "transport_client",
                        "write_connector_secrets",
                        "write_fleet_secrets",
                    ],
                ]
            ]
        ] = None,
        description: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        global_: t.Optional[t.Mapping[str, t.Any]] = None,
        human: t.Optional[bool] = None,
        indices: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        remote_cluster: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        remote_indices: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        run_as: t.Optional[t.Sequence[str]] = None,
        transient_metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update roles.</p>
          <p>The role management APIs are generally the preferred way to manage roles in the native realm, rather than using file-based role management.
          The create or update roles API cannot update roles that are defined in roles files.
          File-based role management is not available in Elastic Serverless.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-role>`_

        :param name: The name of the role that is being created or updated. On Elasticsearch
            Serverless, the role name must begin with a letter or digit and can only
            contain letters, digits and the characters '_', '-', and '.'. Each role must
            have a unique name, as this will serve as the identifier for that role.
        :param applications: A list of application privilege entries.
        :param cluster: A list of cluster privileges. These privileges define the cluster-level
            actions for users with this role.
        :param description: Optional description of the role descriptor
        :param global_: An object defining global privileges. A global privilege is a
            form of cluster privilege that is request-aware. Support for global privileges
            is currently limited to the management of application privileges.
        :param indices: A list of indices permissions entries.
        :param metadata: Optional metadata. Within the metadata object, keys that begin
            with an underscore (`_`) are reserved for system use.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        :param remote_cluster: A list of remote cluster permissions entries.
        :param remote_indices: A list of remote indices permissions entries. NOTE: Remote
            indices are effective for remote clusters configured with the API key based
            model. They have no effect for remote clusters configured with the certificate
            based model.
        :param run_as: A list of users that the owners of this role can impersonate.
            *Note*: in Serverless, the run-as feature is disabled. For API compatibility,
            you can still specify an empty `run_as` field, but a non-empty list will
            be rejected.
        :param transient_metadata: Indicates roles that might be incompatible with the
            current cluster license, specifically roles with document and field level
            security. When the cluster license doesn’t allow certain features for a given
            role, this parameter is updated dynamically to list the incompatible features.
            If `enabled` is `false`, the role is ignored, but is still listed in the
            response from the authenticate API.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_security/role/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if applications is not None:
                __body["applications"] = applications
            if cluster is not None:
                __body["cluster"] = cluster
            if description is not None:
                __body["description"] = description
            if global_ is not None:
                __body["global"] = global_
            if indices is not None:
                __body["indices"] = indices
            if metadata is not None:
                __body["metadata"] = metadata
            if remote_cluster is not None:
                __body["remote_cluster"] = remote_cluster
            if remote_indices is not None:
                __body["remote_indices"] = remote_indices
            if run_as is not None:
                __body["run_as"] = run_as
            if transient_metadata is not None:
                __body["transient_metadata"] = transient_metadata
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.put_role",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "enabled",
            "metadata",
            "role_templates",
            "roles",
            "rules",
            "run_as",
        ),
    )
    async def put_role_mapping(
        self,
        *,
        name: str,
        enabled: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        role_templates: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        roles: t.Optional[t.Sequence[str]] = None,
        rules: t.Optional[t.Mapping[str, t.Any]] = None,
        run_as: t.Optional[t.Sequence[str]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update role mappings.</p>
          <p>Role mappings define which roles are assigned to each user.
          Each mapping has rules that identify users and a list of roles that are granted to those users.
          The role mapping APIs are generally the preferred way to manage role mappings rather than using role mapping files. The create or update role mappings API cannot update role mappings that are defined in role mapping files.</p>
          <p>NOTE: This API does not create roles. Rather, it maps users to existing roles.
          Roles can be created by using the create or update roles API or roles files.</p>
          <p><strong>Role templates</strong></p>
          <p>The most common use for role mappings is to create a mapping from a known value on the user to a fixed role name.
          For example, all users in the <code>cn=admin,dc=example,dc=com</code> LDAP group should be given the superuser role in Elasticsearch.
          The <code>roles</code> field is used for this purpose.</p>
          <p>For more complex needs, it is possible to use Mustache templates to dynamically determine the names of the roles that should be granted to the user.
          The <code>role_templates</code> field is used for this purpose.</p>
          <p>NOTE: To use role templates successfully, the relevant scripting feature must be enabled.
          Otherwise, all attempts to create a role mapping with role templates fail.</p>
          <p>All of the user fields that are available in the role mapping rules are also available in the role templates.
          Thus it is possible to assign a user to a role that reflects their username, their groups, or the name of the realm to which they authenticated.</p>
          <p>By default a template is evaluated to produce a single string that is the name of the role which should be assigned to the user.
          If the format of the template is set to &quot;json&quot; then the template is expected to produce a JSON string or an array of JSON strings for the role names.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-role-mapping>`_

        :param name: The distinct name that identifies the role mapping. The name is
            used solely as an identifier to facilitate interaction via the API; it does
            not affect the behavior of the mapping in any way.
        :param enabled: Mappings that have `enabled` set to `false` are ignored when
            role mapping is performed.
        :param metadata: Additional metadata that helps define which roles are assigned
            to each user. Within the metadata object, keys beginning with `_` are reserved
            for system usage.
        :param refresh: If `true` (the default) then refresh the affected shards to make
            this operation visible to search, if `wait_for` then wait for a refresh to
            make this operation visible to search, if `false` then do nothing with refreshes.
        :param role_templates: A list of Mustache templates that will be evaluated to
            determine the roles names that should granted to the users that match the
            role mapping rules. Exactly one of `roles` or `role_templates` must be specified.
        :param roles: A list of role names that are granted to the users that match the
            role mapping rules. Exactly one of `roles` or `role_templates` must be specified.
        :param rules: The rules that determine which users should be matched by the mapping.
            A rule is a logical condition that is expressed by using a JSON DSL.
        :param run_as:
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_security/role_mapping/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if enabled is not None:
                __body["enabled"] = enabled
            if metadata is not None:
                __body["metadata"] = metadata
            if role_templates is not None:
                __body["role_templates"] = role_templates
            if roles is not None:
                __body["roles"] = roles
            if rules is not None:
                __body["rules"] = rules
            if run_as is not None:
                __body["run_as"] = run_as
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.put_role_mapping",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "email",
            "enabled",
            "full_name",
            "metadata",
            "password",
            "password_hash",
            "roles",
        ),
    )
    async def put_user(
        self,
        *,
        username: str,
        email: t.Optional[t.Union[None, str]] = None,
        enabled: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        full_name: t.Optional[t.Union[None, str]] = None,
        human: t.Optional[bool] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        password: t.Optional[str] = None,
        password_hash: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        roles: t.Optional[t.Sequence[str]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update users.</p>
          <p>Add and update users in the native realm.
          A password is required for adding a new user but is optional when updating an existing user.
          To change a user's password without updating any other fields, use the change password API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-put-user>`_

        :param username: An identifier for the user. NOTE: Usernames must be at least
            1 and no more than 507 characters. They can contain alphanumeric characters
            (a-z, A-Z, 0-9), spaces, punctuation, and printable symbols in the Basic
            Latin (ASCII) block. Leading or trailing whitespace is not allowed.
        :param email: The email of the user.
        :param enabled: Specifies whether the user is enabled.
        :param full_name: The full name of the user.
        :param metadata: Arbitrary metadata that you want to associate with the user.
        :param password: The user's password. Passwords must be at least 6 characters
            long. When adding a user, one of `password` or `password_hash` is required.
            When updating an existing user, the password is optional, so that other fields
            on the user (such as their roles) may be updated without modifying the user's
            password
        :param password_hash: A hash of the user's password. This must be produced using
            the same hashing algorithm as has been configured for password storage. For
            more details, see the explanation of the `xpack.security.authc.password_hashing.algorithm`
            setting in the user cache and password hash algorithm documentation. Using
            this parameter allows the client to pre-hash the password for performance
            and/or confidentiality reasons. The `password` parameter and the `password_hash`
            parameter cannot be used in the same request.
        :param refresh: Valid values are `true`, `false`, and `wait_for`. These values
            have the same meaning as in the index API, but the default value for this
            API is true.
        :param roles: A set of roles the user has. The roles determine the user's access
            permissions. To create a user without any roles, specify an empty list (`[]`).
        """
        if username in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'username'")
        __path_parts: t.Dict[str, str] = {"username": _quote(username)}
        __path = f'/_security/user/{__path_parts["username"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if email is not None:
                __body["email"] = email
            if enabled is not None:
                __body["enabled"] = enabled
            if full_name is not None:
                __body["full_name"] = full_name
            if metadata is not None:
                __body["metadata"] = metadata
            if password is not None:
                __body["password"] = password
            if password_hash is not None:
                __body["password_hash"] = password_hash
            if roles is not None:
                __body["roles"] = roles
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.put_user",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "aggregations",
            "aggs",
            "from_",
            "query",
            "search_after",
            "size",
            "sort",
        ),
        parameter_aliases={"from": "from_"},
    )
    async def query_api_keys(
        self,
        *,
        aggregations: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        aggs: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        search_after: t.Optional[
            t.Sequence[t.Union[None, bool, float, int, str]]
        ] = None,
        size: t.Optional[int] = None,
        sort: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Mapping[str, t.Any]]],
                t.Union[str, t.Mapping[str, t.Any]],
            ]
        ] = None,
        typed_keys: t.Optional[bool] = None,
        with_limited_by: t.Optional[bool] = None,
        with_profile_uid: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Find API keys with a query.</p>
          <p>Get a paginated list of API keys and their information.
          You can optionally filter the results with a query.</p>
          <p>To use this API, you must have at least the <code>manage_own_api_key</code> or the <code>read_security</code> cluster privileges.
          If you have only the <code>manage_own_api_key</code> privilege, this API returns only the API keys that you own.
          If you have the <code>read_security</code>, <code>manage_api_key</code>, or greater privileges (including <code>manage_security</code>), this API returns all API keys regardless of ownership.
          Refer to the linked documentation for examples of how to find API keys:</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-query-api-keys>`_

        :param aggregations: Any aggregations to run over the corpus of returned API
            keys. Aggregations and queries work together. Aggregations are computed only
            on the API keys that match the query. This supports only a subset of aggregation
            types, namely: `terms`, `range`, `date_range`, `missing`, `cardinality`,
            `value_count`, `composite`, `filter`, and `filters`. Additionally, aggregations
            only run over the same subset of fields that query works with.
        :param aggs: Any aggregations to run over the corpus of returned API keys. Aggregations
            and queries work together. Aggregations are computed only on the API keys
            that match the query. This supports only a subset of aggregation types, namely:
            `terms`, `range`, `date_range`, `missing`, `cardinality`, `value_count`,
            `composite`, `filter`, and `filters`. Additionally, aggregations only run
            over the same subset of fields that query works with.
        :param from_: The starting document offset. It must not be negative. By default,
            you cannot page through more than 10,000 hits using the `from` and `size`
            parameters. To page through more hits, use the `search_after` parameter.
        :param query: A query to filter which API keys to return. If the query parameter
            is missing, it is equivalent to a `match_all` query. The query supports a
            subset of query types, including `match_all`, `bool`, `term`, `terms`, `match`,
            `ids`, `prefix`, `wildcard`, `exists`, `range`, and `simple_query_string`.
            You can query the following public information associated with an API key:
            `id`, `type`, `name`, `creation`, `expiration`, `invalidated`, `invalidation`,
            `username`, `realm`, and `metadata`. NOTE: The queryable string values associated
            with API keys are internally mapped as keywords. Consequently, if no `analyzer`
            parameter is specified for a `match` query, then the provided match query
            string is interpreted as a single keyword value. Such a match query is hence
            equivalent to a `term` query.
        :param search_after: The search after definition.
        :param size: The number of hits to return. It must not be negative. The `size`
            parameter can be set to `0`, in which case no API key matches are returned,
            only the aggregation results. By default, you cannot page through more than
            10,000 hits using the `from` and `size` parameters. To page through more
            hits, use the `search_after` parameter.
        :param sort: The sort definition. Other than `id`, all public fields of an API
            key are eligible for sorting. In addition, sort can also be applied to the
            `_doc` field to sort by index order.
        :param typed_keys: Determines whether aggregation names are prefixed by their
            respective types in the response.
        :param with_limited_by: Return the snapshot of the owner user's role descriptors
            associated with the API key. An API key's actual permission is the intersection
            of its assigned role descriptors and the owner user's role descriptors (effectively
            limited by it). An API key cannot retrieve any API key’s limited-by role
            descriptors (including itself) unless it has `manage_api_key` or higher privileges.
        :param with_profile_uid: Determines whether to also retrieve the profile UID
            for the API key owner principal. If it exists, the profile UID is returned
            under the `profile_uid` response field for each API key.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/_query/api_key"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        # The 'sort' parameter with a colon can't be encoded to the body.
        if sort is not None and (
            (isinstance(sort, str) and ":" in sort)
            or (
                isinstance(sort, (list, tuple))
                and all(isinstance(_x, str) for _x in sort)
                and any(":" in _x for _x in sort)
            )
        ):
            __query["sort"] = sort
            sort = None
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if typed_keys is not None:
            __query["typed_keys"] = typed_keys
        if with_limited_by is not None:
            __query["with_limited_by"] = with_limited_by
        if with_profile_uid is not None:
            __query["with_profile_uid"] = with_profile_uid
        if not __body:
            if aggregations is not None:
                __body["aggregations"] = aggregations
            if aggs is not None:
                __body["aggs"] = aggs
            if from_ is not None:
                __body["from"] = from_
            if query is not None:
                __body["query"] = query
            if search_after is not None:
                __body["search_after"] = search_after
            if size is not None:
                __body["size"] = size
            if sort is not None:
                __body["sort"] = sort
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.query_api_keys",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("from_", "query", "search_after", "size", "sort"),
        parameter_aliases={"from": "from_"},
    )
    async def query_role(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        search_after: t.Optional[
            t.Sequence[t.Union[None, bool, float, int, str]]
        ] = None,
        size: t.Optional[int] = None,
        sort: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Mapping[str, t.Any]]],
                t.Union[str, t.Mapping[str, t.Any]],
            ]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Find roles with a query.</p>
          <p>Get roles in a paginated manner.
          The role management APIs are generally the preferred way to manage roles, rather than using file-based role management.
          The query roles API does not retrieve roles that are defined in roles files, nor built-in ones.
          You can optionally filter the results with a query.
          Also, the results can be paginated and sorted.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-query-role>`_

        :param from_: The starting document offset. It must not be negative. By default,
            you cannot page through more than 10,000 hits using the `from` and `size`
            parameters. To page through more hits, use the `search_after` parameter.
        :param query: A query to filter which roles to return. If the query parameter
            is missing, it is equivalent to a `match_all` query. The query supports a
            subset of query types, including `match_all`, `bool`, `term`, `terms`, `match`,
            `ids`, `prefix`, `wildcard`, `exists`, `range`, and `simple_query_string`.
            You can query the following information associated with roles: `name`, `description`,
            `metadata`, `applications.application`, `applications.privileges`, and `applications.resources`.
        :param search_after: The search after definition.
        :param size: The number of hits to return. It must not be negative. By default,
            you cannot page through more than 10,000 hits using the `from` and `size`
            parameters. To page through more hits, use the `search_after` parameter.
        :param sort: The sort definition. You can sort on `username`, `roles`, or `enabled`.
            In addition, sort can also be applied to the `_doc` field to sort by index
            order.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/_query/role"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if from_ is not None:
                __body["from"] = from_
            if query is not None:
                __body["query"] = query
            if search_after is not None:
                __body["search_after"] = search_after
            if size is not None:
                __body["size"] = size
            if sort is not None:
                __body["sort"] = sort
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.query_role",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("from_", "query", "search_after", "size", "sort"),
        parameter_aliases={"from": "from_"},
    )
    async def query_user(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        search_after: t.Optional[
            t.Sequence[t.Union[None, bool, float, int, str]]
        ] = None,
        size: t.Optional[int] = None,
        sort: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Mapping[str, t.Any]]],
                t.Union[str, t.Mapping[str, t.Any]],
            ]
        ] = None,
        with_profile_uid: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Find users with a query.</p>
          <p>Get information for users in a paginated manner.
          You can optionally filter the results with a query.</p>
          <p>NOTE: As opposed to the get user API, built-in users are excluded from the result.
          This API is only for native users.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-query-user>`_

        :param from_: The starting document offset. It must not be negative. By default,
            you cannot page through more than 10,000 hits using the `from` and `size`
            parameters. To page through more hits, use the `search_after` parameter.
        :param query: A query to filter which users to return. If the query parameter
            is missing, it is equivalent to a `match_all` query. The query supports a
            subset of query types, including `match_all`, `bool`, `term`, `terms`, `match`,
            `ids`, `prefix`, `wildcard`, `exists`, `range`, and `simple_query_string`.
            You can query the following information associated with user: `username`,
            `roles`, `enabled`, `full_name`, and `email`.
        :param search_after: The search after definition
        :param size: The number of hits to return. It must not be negative. By default,
            you cannot page through more than 10,000 hits using the `from` and `size`
            parameters. To page through more hits, use the `search_after` parameter.
        :param sort: The sort definition. Fields eligible for sorting are: `username`,
            `roles`, `enabled`. In addition, sort can also be applied to the `_doc` field
            to sort by index order.
        :param with_profile_uid: Determines whether to retrieve the user profile UID,
            if it exists, for the users.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/_query/user"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if with_profile_uid is not None:
            __query["with_profile_uid"] = with_profile_uid
        if not __body:
            if from_ is not None:
                __body["from"] = from_
            if query is not None:
                __body["query"] = query
            if search_after is not None:
                __body["search_after"] = search_after
            if size is not None:
                __body["size"] = size
            if sort is not None:
                __body["sort"] = sort
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.query_user",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("content", "ids", "realm"),
    )
    async def saml_authenticate(
        self,
        *,
        content: t.Optional[str] = None,
        ids: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        realm: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Authenticate SAML.</p>
          <p>Submit a SAML response message to Elasticsearch for consumption.</p>
          <p>NOTE: This API is intended for use by custom web applications other than Kibana.
          If you are using Kibana, refer to the documentation for configuring SAML single-sign-on on the Elastic Stack.</p>
          <p>The SAML message that is submitted can be:</p>
          <ul>
          <li>A response to a SAML authentication request that was previously created using the SAML prepare authentication API.</li>
          <li>An unsolicited SAML message in the case of an IdP-initiated single sign-on (SSO) flow.</li>
          </ul>
          <p>In either case, the SAML message needs to be a base64 encoded XML document with a root element of <code>&lt;Response&gt;</code>.</p>
          <p>After successful validation, Elasticsearch responds with an Elasticsearch internal access token and refresh token that can be subsequently used for authentication.
          This API endpoint essentially exchanges SAML responses that indicate successful authentication in the IdP for Elasticsearch access and refresh tokens, which can be used for authentication against Elasticsearch.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-saml-authenticate>`_

        :param content: The SAML response as it was sent by the user's browser, usually
            a Base64 encoded XML document.
        :param ids: A JSON array with all the valid SAML Request Ids that the caller
            of the API has for the current user.
        :param realm: The name of the realm that should authenticate the SAML response.
            Useful in cases where many SAML realms are defined.
        """
        if content is None and body is None:
            raise ValueError("Empty value passed for parameter 'content'")
        if ids is None and body is None:
            raise ValueError("Empty value passed for parameter 'ids'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/saml/authenticate"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if content is not None:
                __body["content"] = content
            if ids is not None:
                __body["ids"] = ids
            if realm is not None:
                __body["realm"] = realm
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.saml_authenticate",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("ids", "realm", "content", "query_string"),
    )
    async def saml_complete_logout(
        self,
        *,
        ids: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        realm: t.Optional[str] = None,
        content: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        query_string: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Logout of SAML completely.</p>
          <p>Verifies the logout response sent from the SAML IdP.</p>
          <p>NOTE: This API is intended for use by custom web applications other than Kibana.
          If you are using Kibana, refer to the documentation for configuring SAML single-sign-on on the Elastic Stack.</p>
          <p>The SAML IdP may send a logout response back to the SP after handling the SP-initiated SAML Single Logout.
          This API verifies the response by ensuring the content is relevant and validating its signature.
          An empty response is returned if the verification process is successful.
          The response can be sent by the IdP with either the HTTP-Redirect or the HTTP-Post binding.
          The caller of this API must prepare the request accordingly so that this API can handle either of them.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-saml-complete-logout>`_

        :param ids: A JSON array with all the valid SAML Request Ids that the caller
            of the API has for the current user.
        :param realm: The name of the SAML realm in Elasticsearch for which the configuration
            is used to verify the logout response.
        :param content: If the SAML IdP sends the logout response with the HTTP-Post
            binding, this field must be set to the value of the SAMLResponse form parameter
            from the logout response.
        :param query_string: If the SAML IdP sends the logout response with the HTTP-Redirect
            binding, this field must be set to the query string of the redirect URI.
        """
        if ids is None and body is None:
            raise ValueError("Empty value passed for parameter 'ids'")
        if realm is None and body is None:
            raise ValueError("Empty value passed for parameter 'realm'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/saml/complete_logout"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if ids is not None:
                __body["ids"] = ids
            if realm is not None:
                __body["realm"] = realm
            if content is not None:
                __body["content"] = content
            if query_string is not None:
                __body["query_string"] = query_string
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.saml_complete_logout",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("query_string", "acs", "realm"),
    )
    async def saml_invalidate(
        self,
        *,
        query_string: t.Optional[str] = None,
        acs: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        realm: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Invalidate SAML.</p>
          <p>Submit a SAML LogoutRequest message to Elasticsearch for consumption.</p>
          <p>NOTE: This API is intended for use by custom web applications other than Kibana.
          If you are using Kibana, refer to the documentation for configuring SAML single-sign-on on the Elastic Stack.</p>
          <p>The logout request comes from the SAML IdP during an IdP initiated Single Logout.
          The custom web application can use this API to have Elasticsearch process the <code>LogoutRequest</code>.
          After successful validation of the request, Elasticsearch invalidates the access token and refresh token that corresponds to that specific SAML principal and provides a URL that contains a SAML LogoutResponse message.
          Thus the user can be redirected back to their IdP.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-saml-invalidate>`_

        :param query_string: The query part of the URL that the user was redirected to
            by the SAML IdP to initiate the Single Logout. This query should include
            a single parameter named `SAMLRequest` that contains a SAML logout request
            that is deflated and Base64 encoded. If the SAML IdP has signed the logout
            request, the URL should include two extra parameters named `SigAlg` and `Signature`
            that contain the algorithm used for the signature and the signature value
            itself. In order for Elasticsearch to be able to verify the IdP's signature,
            the value of the `query_string` field must be an exact match to the string
            provided by the browser. The client application must not attempt to parse
            or process the string in any way.
        :param acs: The Assertion Consumer Service URL that matches the one of the SAML
            realm in Elasticsearch that should be used. You must specify either this
            parameter or the `realm` parameter.
        :param realm: The name of the SAML realm in Elasticsearch the configuration.
            You must specify either this parameter or the `acs` parameter.
        """
        if query_string is None and body is None:
            raise ValueError("Empty value passed for parameter 'query_string'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/saml/invalidate"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if query_string is not None:
                __body["query_string"] = query_string
            if acs is not None:
                __body["acs"] = acs
            if realm is not None:
                __body["realm"] = realm
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.saml_invalidate",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("token", "refresh_token"),
    )
    async def saml_logout(
        self,
        *,
        token: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh_token: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Logout of SAML.</p>
          <p>Submits a request to invalidate an access token and refresh token.</p>
          <p>NOTE: This API is intended for use by custom web applications other than Kibana.
          If you are using Kibana, refer to the documentation for configuring SAML single-sign-on on the Elastic Stack.</p>
          <p>This API invalidates the tokens that were generated for a user by the SAML authenticate API.
          If the SAML realm in Elasticsearch is configured accordingly and the SAML IdP supports this, the Elasticsearch response contains a URL to redirect the user to the IdP that contains a SAML logout request (starting an SP-initiated SAML Single Logout).</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-saml-logout>`_

        :param token: The access token that was returned as a response to calling the
            SAML authenticate API. Alternatively, the most recent token that was received
            after refreshing the original one by using a `refresh_token`.
        :param refresh_token: The refresh token that was returned as a response to calling
            the SAML authenticate API. Alternatively, the most recent refresh token that
            was received after refreshing the original access token.
        """
        if token is None and body is None:
            raise ValueError("Empty value passed for parameter 'token'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/saml/logout"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if token is not None:
                __body["token"] = token
            if refresh_token is not None:
                __body["refresh_token"] = refresh_token
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.saml_logout",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("acs", "realm", "relay_state"),
    )
    async def saml_prepare_authentication(
        self,
        *,
        acs: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        realm: t.Optional[str] = None,
        relay_state: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Prepare SAML authentication.</p>
          <p>Create a SAML authentication request (<code>&lt;AuthnRequest&gt;</code>) as a URL string based on the configuration of the respective SAML realm in Elasticsearch.</p>
          <p>NOTE: This API is intended for use by custom web applications other than Kibana.
          If you are using Kibana, refer to the documentation for configuring SAML single-sign-on on the Elastic Stack.</p>
          <p>This API returns a URL pointing to the SAML Identity Provider.
          You can use the URL to redirect the browser of the user in order to continue the authentication process.
          The URL includes a single parameter named <code>SAMLRequest</code>, which contains a SAML Authentication request that is deflated and Base64 encoded.
          If the configuration dictates that SAML authentication requests should be signed, the URL has two extra parameters named <code>SigAlg</code> and <code>Signature</code>.
          These parameters contain the algorithm used for the signature and the signature value itself.
          It also returns a random string that uniquely identifies this SAML Authentication request.
          The caller of this API needs to store this identifier as it needs to be used in a following step of the authentication process.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-saml-prepare-authentication>`_

        :param acs: The Assertion Consumer Service URL that matches the one of the SAML
            realms in Elasticsearch. The realm is used to generate the authentication
            request. You must specify either this parameter or the `realm` parameter.
        :param realm: The name of the SAML realm in Elasticsearch for which the configuration
            is used to generate the authentication request. You must specify either this
            parameter or the `acs` parameter.
        :param relay_state: A string that will be included in the redirect URL that this
            API returns as the `RelayState` query parameter. If the Authentication Request
            is signed, this value is used as part of the signature computation.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/saml/prepare"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if acs is not None:
                __body["acs"] = acs
            if realm is not None:
                __body["realm"] = realm
            if relay_state is not None:
                __body["relay_state"] = relay_state
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.saml_prepare_authentication",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def saml_service_provider_metadata(
        self,
        *,
        realm_name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create SAML service provider metadata.</p>
          <p>Generate SAML metadata for a SAML 2.0 Service Provider.</p>
          <p>The SAML 2.0 specification provides a mechanism for Service Providers to describe their capabilities and configuration using a metadata file.
          This API generates Service Provider metadata based on the configuration of a SAML realm in Elasticsearch.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-saml-service-provider-metadata>`_

        :param realm_name: The name of the SAML realm in Elasticsearch.
        """
        if realm_name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'realm_name'")
        __path_parts: t.Dict[str, str] = {"realm_name": _quote(realm_name)}
        __path = f'/_security/saml/metadata/{__path_parts["realm_name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="security.saml_service_provider_metadata",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("data", "hint", "name", "size"),
    )
    async def suggest_user_profiles(
        self,
        *,
        data: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        hint: t.Optional[t.Mapping[str, t.Any]] = None,
        human: t.Optional[bool] = None,
        name: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        size: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Suggest a user profile.</p>
          <p>Get suggestions for user profiles that match specified search criteria.</p>
          <p>NOTE: The user profile feature is designed only for use by Kibana and Elastic's Observability, Enterprise Search, and Elastic Security solutions.
          Individual users and external applications should not call this API directly.
          Elastic reserves the right to change or remove this feature in future releases without prior notice.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-suggest-user-profiles>`_

        :param data: A comma-separated list of filters for the `data` field of the profile
            document. To return all content use `data=*`. To return a subset of content,
            use `data=<key>` to retrieve content nested under the specified `<key>`.
            By default, the API returns no `data` content. It is an error to specify
            `data` as both the query parameter and the request body field.
        :param hint: Extra search criteria to improve relevance of the suggestion result.
            Profiles matching the spcified hint are ranked higher in the response. Profiles
            not matching the hint aren't excluded from the response as long as the profile
            matches the `name` field query.
        :param name: A query string used to match name-related fields in user profile
            documents. Name-related fields are the user's `username`, `full_name`, and
            `email`.
        :param size: The number of profiles to return.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/profile/_suggest"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if data is not None:
                __body["data"] = data
            if hint is not None:
                __body["hint"] = hint
            if name is not None:
                __body["name"] = name
            if size is not None:
                __body["size"] = size
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.suggest_user_profiles",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("expiration", "metadata", "role_descriptors"),
    )
    async def update_api_key(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        expiration: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        role_descriptors: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update an API key.</p>
          <p>Update attributes of an existing API key.
          This API supports updates to an API key's access scope, expiration, and metadata.</p>
          <p>To use this API, you must have at least the <code>manage_own_api_key</code> cluster privilege.
          Users can only update API keys that they created or that were granted to them.
          To update another user’s API key, use the <code>run_as</code> feature to submit a request on behalf of another user.</p>
          <p>IMPORTANT: It's not possible to use an API key as the authentication credential for this API. The owner user’s credentials are required.</p>
          <p>Use this API to update API keys created by the create API key or grant API Key APIs.
          If you need to apply the same update to many API keys, you can use the bulk update API keys API to reduce overhead.
          It's not possible to update expired API keys or API keys that have been invalidated by the invalidate API key API.</p>
          <p>The access scope of an API key is derived from the <code>role_descriptors</code> you specify in the request and a snapshot of the owner user's permissions at the time of the request.
          The snapshot of the owner's permissions is updated automatically on every call.</p>
          <p>IMPORTANT: If you don't specify <code>role_descriptors</code> in the request, a call to this API might still change the API key's access scope.
          This change can occur if the owner user's permissions have changed since the API key was created or last modified.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-api-key>`_

        :param id: The ID of the API key to update.
        :param expiration: The expiration time for the API key. By default, API keys
            never expire. This property can be omitted to leave the expiration unchanged.
        :param metadata: Arbitrary metadata that you want to associate with the API key.
            It supports a nested data structure. Within the metadata object, keys beginning
            with `_` are reserved for system usage. When specified, this value fully
            replaces the metadata previously associated with the API key.
        :param role_descriptors: The role descriptors to assign to this API key. The
            API key's effective permissions are an intersection of its assigned privileges
            and the point in time snapshot of permissions of the owner user. You can
            assign new privileges by specifying them in this parameter. To remove assigned
            privileges, you can supply an empty `role_descriptors` parameter, that is
            to say, an empty object `{}`. If an API key has no assigned privileges, it
            inherits the owner user's full permissions. The snapshot of the owner's permissions
            is always updated, whether you supply the `role_descriptors` parameter or
            not. The structure of a role descriptor is the same as the request for the
            create API keys API.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_security/api_key/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if expiration is not None:
                __body["expiration"] = expiration
            if metadata is not None:
                __body["metadata"] = metadata
            if role_descriptors is not None:
                __body["role_descriptors"] = role_descriptors
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.update_api_key",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("access", "expiration", "metadata"),
    )
    async def update_cross_cluster_api_key(
        self,
        *,
        id: str,
        access: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        expiration: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        metadata: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update a cross-cluster API key.</p>
          <p>Update the attributes of an existing cross-cluster API key, which is used for API key based remote cluster access.</p>
          <p>To use this API, you must have at least the <code>manage_security</code> cluster privilege.
          Users can only update API keys that they created.
          To update another user's API key, use the <code>run_as</code> feature to submit a request on behalf of another user.</p>
          <p>IMPORTANT: It's not possible to use an API key as the authentication credential for this API.
          To update an API key, the owner user's credentials are required.</p>
          <p>It's not possible to update expired API keys, or API keys that have been invalidated by the invalidate API key API.</p>
          <p>This API supports updates to an API key's access scope, metadata, and expiration.
          The owner user's information, such as the <code>username</code> and <code>realm</code>, is also updated automatically on every call.</p>
          <p>NOTE: This API cannot update REST API keys, which should be updated by either the update API key or bulk update API keys API.</p>
          <p>To learn more about how to use this API, refer to the <a href="https://www.elastic.co/docs/reference/elasticsearch/rest-apis/update-cc-api-key-examples">Update cross cluter API key API examples page</a>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-cross-cluster-api-key>`_

        :param id: The ID of the cross-cluster API key to update.
        :param access: The access to be granted to this API key. The access is composed
            of permissions for cross cluster search and cross cluster replication. At
            least one of them must be specified. When specified, the new access assignment
            fully replaces the previously assigned access.
        :param expiration: The expiration time for the API key. By default, API keys
            never expire. This property can be omitted to leave the value unchanged.
        :param metadata: Arbitrary metadata that you want to associate with the API key.
            It supports nested data structure. Within the metadata object, keys beginning
            with `_` are reserved for system usage. When specified, this information
            fully replaces metadata previously associated with the API key.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        if access is None and body is None:
            raise ValueError("Empty value passed for parameter 'access'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_security/cross_cluster/api_key/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if access is not None:
                __body["access"] = access
            if expiration is not None:
                __body["expiration"] = expiration
            if metadata is not None:
                __body["metadata"] = metadata
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.update_cross_cluster_api_key",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("security", "security_profile", "security_tokens"),
        parameter_aliases={
            "security-profile": "security_profile",
            "security-tokens": "security_tokens",
        },
    )
    async def update_settings(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        security: t.Optional[t.Mapping[str, t.Any]] = None,
        security_profile: t.Optional[t.Mapping[str, t.Any]] = None,
        security_tokens: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update security index settings.</p>
          <p>Update the user-configurable settings for the security internal index (<code>.security</code> and associated indices). Only a subset of settings are allowed to be modified. This includes <code>index.auto_expand_replicas</code> and <code>index.number_of_replicas</code>.</p>
          <p>NOTE: If <code>index.auto_expand_replicas</code> is set, <code>index.number_of_replicas</code> will be ignored during updates.</p>
          <p>If a specific index is not in use on the system and settings are provided for it, the request will be rejected.
          This API does not yet support configuring the settings for indices before they are in use.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-settings>`_

        :param master_timeout: The period to wait for a connection to the master node.
            If no response is received before the timeout expires, the request fails
            and returns an error.
        :param security: Settings for the index used for most security configuration,
            including native realm users and roles configured with the API.
        :param security_profile: Settings for the index used to store profile information.
        :param security_tokens: Settings for the index used to store tokens.
        :param timeout: The period to wait for a response. If no response is received
            before the timeout expires, the request fails and returns an error.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_security/settings"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if security is not None:
                __body["security"] = security
            if security_profile is not None:
                __body["security-profile"] = security_profile
            if security_tokens is not None:
                __body["security-tokens"] = security_tokens
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.update_settings",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("data", "labels"),
    )
    async def update_user_profile_data(
        self,
        *,
        uid: str,
        data: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        if_primary_term: t.Optional[int] = None,
        if_seq_no: t.Optional[int] = None,
        labels: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update user profile data.</p>
          <p>Update specific data for the user profile that is associated with a unique ID.</p>
          <p>NOTE: The user profile feature is designed only for use by Kibana and Elastic's Observability, Enterprise Search, and Elastic Security solutions.
          Individual users and external applications should not call this API directly.
          Elastic reserves the right to change or remove this feature in future releases without prior notice.</p>
          <p>To use this API, you must have one of the following privileges:</p>
          <ul>
          <li>The <code>manage_user_profile</code> cluster privilege.</li>
          <li>The <code>update_profile_data</code> global privilege for the namespaces that are referenced in the request.</li>
          </ul>
          <p>This API updates the <code>labels</code> and <code>data</code> fields of an existing user profile document with JSON objects.
          New keys and their values are added to the profile document and conflicting keys are replaced by data that's included in the request.</p>
          <p>For both labels and data, content is namespaced by the top-level fields.
          The <code>update_profile_data</code> global privilege grants privileges for updating only the allowed namespaces.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-update-user-profile-data>`_

        :param uid: A unique identifier for the user profile.
        :param data: Non-searchable data that you want to associate with the user profile.
            This field supports a nested data structure. Within the `data` object, top-level
            keys cannot begin with an underscore (`_`) or contain a period (`.`). The
            data object is not searchable, but can be retrieved with the get user profile
            API.
        :param if_primary_term: Only perform the operation if the document has this primary
            term.
        :param if_seq_no: Only perform the operation if the document has this sequence
            number.
        :param labels: Searchable data that you want to associate with the user profile.
            This field supports a nested data structure. Within the labels object, top-level
            keys cannot begin with an underscore (`_`) or contain a period (`.`).
        :param refresh: If 'true', Elasticsearch refreshes the affected shards to make
            this operation visible to search. If 'wait_for', it waits for a refresh to
            make this operation visible to search. If 'false', nothing is done with refreshes.
        """
        if uid in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'uid'")
        __path_parts: t.Dict[str, str] = {"uid": _quote(uid)}
        __path = f'/_security/profile/{__path_parts["uid"]}/_data'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if if_primary_term is not None:
            __query["if_primary_term"] = if_primary_term
        if if_seq_no is not None:
            __query["if_seq_no"] = if_seq_no
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if data is not None:
                __body["data"] = data
            if labels is not None:
                __body["labels"] = labels
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="security.update_user_profile_data",
            path_parts=__path_parts,
        )
