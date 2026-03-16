#!/usr/bin/env python
"""LDAP methods module."""
from hvac import exceptions, utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "ldap"


class Ldap(VaultApiBase):
    """LDAP Auth Method (API).

    Reference: https://www.vaultproject.io/api/auth/ldap/index.html
    """

    @utils.aliased_parameter(
        "userdn", "user_dn", removed_in_version="3.0.0", position=1
    )
    @utils.aliased_parameter(
        "groupdn", "group_dn", removed_in_version="3.0.0", position=2
    )
    @utils.aliased_parameter(
        "binddn", "bind_dn", removed_in_version="3.0.0", position=10
    )
    @utils.aliased_parameter(
        "bindpass", "bind_pass", removed_in_version="3.0.0", position=11
    )
    @utils.aliased_parameter(
        "userattr", "user_attr", removed_in_version="3.0.0", position=12
    )
    @utils.aliased_parameter(
        "discoverdn", "discover_dn", removed_in_version="3.0.0", position=13
    )
    @utils.aliased_parameter(
        "upndomain", "upn_domain", removed_in_version="3.0.0", position=15
    )
    @utils.aliased_parameter(
        "groupfilter", "group_filter", removed_in_version="3.0.0", position=16
    )
    @utils.aliased_parameter(
        "groupattr", "group_attr", removed_in_version="3.0.0", position=17
    )
    def configure(
        self,
        userdn=None,
        groupdn=None,
        url=None,
        case_sensitive_names=None,
        starttls=None,
        tls_min_version=None,
        tls_max_version=None,
        insecure_tls=None,
        certificate=None,
        binddn=None,
        bindpass=None,
        userattr=None,
        discoverdn=None,
        deny_null_bind=True,
        upndomain=None,
        groupfilter=None,
        groupattr=None,
        use_token_groups=None,
        token_ttl=None,
        token_max_ttl=None,
        mount_point=DEFAULT_MOUNT_POINT,
        *,
        anonymous_group_search=None,
        client_tls_cert=None,
        client_tls_key=None,
        connection_timeout=None,
        dereference_aliases=None,
        max_page_size=None,
        request_timeout=None,
        token_bound_cidrs=None,
        token_explicit_max_ttl=None,
        token_no_default_policy=None,
        token_num_uses=None,
        token_period=None,
        token_policies=None,
        token_type=None,
        userfilter=None,
        username_as_alias=None,
    ):
        """
        Configure the LDAP auth method.

        Supported methods:
            POST: /auth/{mount_point}/config. Produces: 204 (empty body)

        :param anonymous_group_search: Use anonymous binds when performing LDAP group searches (note: even when true,
            the initial credentials will still be used for the initial connection test).
        :type anonymous_group_search: bool
        :param client_tls_cert: Client certificate to provide to the LDAP server, must be x509 PEM encoded.
        :type client_tls_cert: str | unicode
        :param client_tls_key: Client certificate key to provide to the LDAP server, must be x509 PEM encoded.
        :type client_tls_key: str | unicode
        :param connection_timeout: Timeout, in seconds, when attempting to connect to the LDAP server before trying the
            next URL in the configuration.
        :type connection_timeout: int
        :param dereference_aliases: When aliases should be dereferenced on search operations.
            Accepted values are 'never', 'finding', 'searching', 'always'.
        :type dereference_aliases: str | unicode
        :param max_page_size: If set to a value greater than 0, the LDAP backend will use the LDAP server's paged search
            control to request pages of up to the given size.
        :type max_page_size: int
        :param request_timeout: Timeout, in seconds, for the connection when making requests against the server before
            returning back an error.
        :type request_timeout: str | unicode
        :param token_bound_cidrs: List of CIDR blocks; if set, specifies blocks of IP addresses which can authenticate
            successfully, and ties the resulting token to these blocks as well.
        :type token_bound_cidrs: list
        :param token_explicit_max_ttl: If set, will encode an explicit max TTL onto the token. This is a hard cap even
            if token_ttl and token_max_ttl would otherwise allow a renewal.
        :type token_explicit_max_ttl: str | unicode
        :param token_no_default_policy: If set, the default policy will not be set on generated tokens; otherwise it
            will be added to the policies set in token_policies.
        :type token_no_default_policy: bool
        :param token_num_uses: The maximum number of times a generated token may be used (within its lifetime); 0 means
            unlimited.
        :type token_num_uses: int
        :param token_period: The maximum allowed period value when a periodic token is requested from this role.
        :type token_period: str | unicode
        :param token_policies: List of token policies to encode onto generated tokens.
        :type token_policies: list
        :param token_type: The type of token that should be generated.
        :type token_type: str | unicode
        :param userfilter: An optional LDAP user search filter.
        :type userfilter: str | unicode
        :param username_as_alias: If set to true, forces the auth method to use the username passed by the user as the
            alias name.
        :type username_as_alias: bool
        :param userdn: Base DN under which to perform user search. Example: ou=Users,dc=example,dc=com
        :type userdn: str | unicode
        :param user_dn: Alias for userdn. This alias will be removed in v3.0.0.
        :type user_dn: str | unicode
        :param groupdn: LDAP search base to use for group membership search. This can be the root containing either
            groups or users. Example: ou=Groups,dc=example,dc=com
        :type groupdn: str | unicode
        :param group_dn: Alias for groupdn. This alias will be removed in v3.0.0.
        :type group_dn: str | unicode
        :param url: The LDAP server to connect to. Examples: ldap://ldap.myorg.com, ldaps://ldap.myorg.com:636.
            Multiple URLs can be specified with commas, e.g. ldap://ldap.myorg.com,ldap://ldap2.myorg.com; these will be
            tried in-order.
        :type url: str | unicode
        :param case_sensitive_names: If set, user and group names assigned to policies within the backend will be case
            sensitive. Otherwise, names will be normalized to lower case. Case will still be preserved when sending the
            username to the LDAP server at login time; this is only for matching local user/group definitions.
        :type case_sensitive_names: bool
        :param starttls: If true, issues a StartTLS command after establishing an unencrypted connection.
        :type starttls: bool
        :param tls_min_version: Minimum TLS version to use. Accepted values are tls10, tls11 or tls12.
        :type tls_min_version: str | unicode
        :param tls_max_version: Maximum TLS version to use. Accepted values are tls10, tls11 or tls12.
        :type tls_max_version: str | unicode
        :param insecure_tls: If true, skips LDAP server SSL certificate verification - insecure, use with caution!
        :type insecure_tls: bool
        :param certificate: CA certificate to use when verifying LDAP server certificate, must be x509 PEM encoded.
        :type certificate: str | unicode
        :param binddn: Distinguished name of object to bind when performing user search. Example:
            cn=vault,ou=Users,dc=example,dc=com
        :type binddn: str | unicode
        :param bind_dn: Alias for binddn. This alias will be removed in v3.0.0.
        :type bind_dn: str | unicode
        :param bindpass:  Password to use along with binddn when performing user search.
        :type bindpass: str | unicode
        :param bind_pass: Alias for bindpass. This alias will be removed in v3.0.0.
        :type bind_pass: str | unicode
        :param userattr: Attribute on user attribute object matching the username passed when authenticating. Examples:
            sAMAccountName, cn, uid
        :type userattr: str | unicode
        :param user_attr: Alias for userattr. This alias will be removed in v3.0.0.
        :type user_attr: str | unicode
        :param discoverdn: Use anonymous bind to discover the bind DN of a user.
        :type discoverdn: bool
        :param discover_dn: Alias for discoverdn. This alias will be removed in v3.0.0.
        :type discover_dn: bool
        :param deny_null_bind: This option prevents users from bypassing authentication when providing an empty password.
        :type deny_null_bind: bool
        :param upndomain: The userPrincipalDomain used to construct the UPN string for the authenticating user. The
            constructed UPN will appear as [username]@UPNDomain. Example: example.com, which will cause vault to bind as
            username@example.com.
        :type upndomain: str | unicode
        :param upn_domain: Alias for upndomain. This alias will be removed in v3.0.0.
        :type upn_domain: str | unicode
        :param groupfilter: Go template used when constructing the group membership query. The template can access the
            following context variables: [UserDN, Username]. The default is
            `(|(memberUid={{.Username}})(member={{.UserDN}})(uniqueMember={{.UserDN}}))`, which is compatible with several
            common directory schemas. To support nested group resolution for Active Directory, instead use the following
            query: (&(objectClass=group)(member:1.2.840.113556.1.4.1941:={{.UserDN}})).
        :type groupfilter: str | unicode
        :param group_filter: Alias for groupfilter. This alias will be removed in v3.0.0.
        :type group_filter: str | unicode
        :param groupattr: LDAP attribute to follow on objects returned by groupfilter in order to enumerate user group
            membership. Examples: for groupfilter queries returning group objects, use: cn. For queries returning user
            objects, use: memberOf. The default is cn.
        :type groupattr: str | unicode
        :param group_attr: Alias for groupattr. This alias will be removed in v3.0.0.
        :type group_attr: str | unicode
        :param use_token_groups: If true, groups are resolved through Active Directory tokens. This may speed up nested
            group membership resolution in large directories.
        :type use_token_groups: bool
        :param token_ttl: The incremental lifetime for generated tokens.
        :type token_ttl: str | unicode
        :param token_max_ttl: The maximum lifetime for generated tokens.
        :type token_max_ttl: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the configure request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "url": url,
                "anonymous_group_search": anonymous_group_search,
                "binddn": binddn,
                "bindpass": bindpass,
                "case_sensitive_names": case_sensitive_names,
                "certificate": certificate,
                "client_tls_cert": client_tls_cert,
                "client_tls_key": client_tls_key,
                "connection_timeout": connection_timeout,
                "deny_null_bind": deny_null_bind,
                "dereference_aliases": dereference_aliases,
                "discoverdn": discoverdn,
                "groupattr": groupattr,
                "groupdn": groupdn,
                "groupfilter": groupfilter,
                "insecure_tls": insecure_tls,
                "max_page_size": max_page_size,
                "request_timeout": request_timeout,
                "starttls": starttls,
                "tls_max_version": tls_max_version,
                "tls_min_version": tls_min_version,
                "token_bound_cidrs": token_bound_cidrs,
                "token_explicit_max_ttl": token_explicit_max_ttl,
                "token_max_ttl": token_max_ttl,
                "token_no_default_policy": token_no_default_policy,
                "token_num_uses": token_num_uses,
                "token_period": token_period,
                "token_policies": token_policies,
                "token_ttl": token_ttl,
                "token_type": token_type,
                "upndomain": upndomain,
                "use_token_groups": use_token_groups,
                "userattr": userattr,
                "userdn": userdn,
                "userfilter": userfilter,
                "username_as_alias": username_as_alias,
            }
        )

        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_configuration(self, mount_point=DEFAULT_MOUNT_POINT):
        """
        Retrieve the LDAP configuration for the auth method.

        Supported methods:
            GET: /auth/{mount_point}/config. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_configuration request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config", mount_point=mount_point
        )
        return self._adapter.get(
            url=api_path,
        )

    def create_or_update_group(
        self, name, policies=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """
        Create or update LDAP group policies.

        Supported methods:
            POST: /auth/{mount_point}/groups/{name}. Produces: 204 (empty body)


        :param name: The name of the LDAP group
        :type name: str | unicode
        :param policies: List of policies associated with the group. This parameter is transformed to a comma-delimited
            string before being passed to Vault.
        :type policies: list
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the create_or_update_group request.
        :rtype: requests.Response
        """
        if policies is not None and not isinstance(policies, list):
            error_msg = '"policies" argument must be an instance of list or None, "{policies_type}" provided.'.format(
                policies_type=type(policies),
            )
            raise exceptions.ParamValidationError(error_msg)

        params = {}
        if policies is not None:
            params["policies"] = ",".join(policies)
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/groups/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def list_groups(self, mount_point=DEFAULT_MOUNT_POINT):
        """
        List existing LDAP existing groups that have been created in this auth method.

        Supported methods:
            LIST: /auth/{mount_point}/groups. Produces: 200 application/json


        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the list_groups request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/groups", mount_point=mount_point
        )
        return self._adapter.list(
            url=api_path,
        )

    def read_group(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """
        Read policies associated with a LDAP group.

        Supported methods:
            GET: /auth/{mount_point}/groups/{name}. Produces: 200 application/json


        :param name: The name of the LDAP group
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_group request.
        :rtype: dict
        """
        params = {
            "name": name,
        }
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/groups/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
            json=params,
        )

    def delete_group(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """
        Delete a LDAP group and policy association.

        Supported methods:
            DELETE: /auth/{mount_point}/groups/{name}. Produces: 204 (empty body)


        :param name: The name of the LDAP group
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the delete_group request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/groups/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def create_or_update_user(
        self, username, policies=None, groups=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """
        Create or update LDAP users policies and group associations.

        Supported methods:
            POST: /auth/{mount_point}/users/{username}. Produces: 204 (empty body)


        :param username: The username of the LDAP user
        :type username: str | unicode
        :param policies: List of policies associated with the user. This parameter is transformed to a comma-delimited
            string before being passed to Vault.
        :type policies: str | unicode
        :param groups: List of groups associated with the user. This parameter is transformed to a comma-delimited
            string before being passed to Vault.
        :type groups: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the create_or_update_user request.
        :rtype: requests.Response
        """
        list_required_params = {
            "policies": policies,
            "groups": groups,
        }
        for param_name, param_arg in list_required_params.items():
            if param_arg is not None and not isinstance(param_arg, list):
                error_msg = '"{param_name}" argument must be an instance of list or None, "{param_type}" provided.'.format(
                    param_name=param_name,
                    param_type=type(param_arg),
                )
                raise exceptions.ParamValidationError(error_msg)

        params = {}
        if policies is not None:
            params["policies"] = ",".join(policies)
        if groups is not None:
            params["groups"] = ",".join(groups)
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/users/{username}",
            mount_point=mount_point,
            username=username,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def list_users(self, mount_point=DEFAULT_MOUNT_POINT):
        """
        List existing users in the method.

        Supported methods:
            LIST: /auth/{mount_point}/users. Produces: 200 application/json


        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the list_users request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/users", mount_point=mount_point
        )
        return self._adapter.list(
            url=api_path,
        )

    def read_user(self, username, mount_point=DEFAULT_MOUNT_POINT):
        """
        Read policies associated with a LDAP user.

        Supported methods:
            GET: /auth/{mount_point}/users/{username}. Produces: 200 application/json


        :param username: The username of the LDAP user
        :type username: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_user request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/users/{username}",
            mount_point=mount_point,
            username=username,
        )
        return self._adapter.get(
            url=api_path,
        )

    def delete_user(self, username, mount_point=DEFAULT_MOUNT_POINT):
        """
        Delete a LDAP user and policy association.

        Supported methods:
            DELETE: /auth/{mount_point}/users/{username}. Produces: 204 (empty body)


        :param username: The username of the LDAP user
        :type username: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the delete_user request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/users/{username}",
            mount_point=mount_point,
            username=username,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def login(
        self, username, password, use_token=True, mount_point=DEFAULT_MOUNT_POINT
    ):
        """
        Log in with LDAP credentials.

        Supported methods:
            POST: /auth/{mount_point}/login/{username}. Produces: 200 application/json


        :param username: The username of the LDAP user
        :type username: str | unicode
        :param password: The password for the LDAP user
        :type password: str | unicode
        :param use_token: if True, uses the token in the response received from the auth request to set the "token"
            attribute on the the :py:meth:`hvac.adapters.Adapter` instance under the _adapter Client attribute.
        :type use_token: bool
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the login_with_user request.
        :rtype: requests.Response
        """
        params = {
            "password": password,
        }
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/login/{username}",
            mount_point=mount_point,
            username=username,
        )
        return self._adapter.login(
            url=api_path,
            use_token=use_token,
            json=params,
        )
