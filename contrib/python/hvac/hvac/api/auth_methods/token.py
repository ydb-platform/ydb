#!/usr/bin/env python
"""Token methods module."""
from hvac import utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "token"


class Token(VaultApiBase):
    """Token Auth Method (API).

    Reference: http://localhost:3000/api-docs/auth/token
    """

    def create(
        self,
        id=None,
        role_name=None,
        policies=None,
        meta=None,
        no_parent=False,
        no_default_policy=False,
        renewable=True,
        ttl=None,
        type=None,
        explicit_max_ttl=None,
        display_name="token",
        num_uses=0,
        period=None,
        entity_alias=None,
        wrap_ttl=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create a new token.

        Certain options are only available when called by a root token. If used
        via the /auth/token/create-orphan endpoint, a root token is not required
        to create an orphan token (otherwise set with the no_parent option). If
        used with a role name in the path, the token will be created against the
        specified role name; this may override options set during this call.


        :param id: The ID of the client token. Can only be specified by a root token.
            The ID provided may not contain a `.` character. Otherwise, the
            token ID is a randomly generated value.
        :type id: str
        :param role_name: The name of the token role.
        :type role_name: str
        :param policies: A list of policies for the token. This must be a
            subset of the policies belonging to the token making the request, unless root.
            If not specified, defaults to all the policies of the calling token.
        :type policies: list
        :param meta: A map of string to string valued metadata. This is
            passed through to the audit devices.
        :type meta: map
        :param no_parent: This argument only has effect if used by a root or sudo caller.
            When set to `True`, the token created will not have a parent.
        :type no_parent: bool
        :param no_default_policy: If `True` the default policy will not be contained in this token's policy set.
        :type no_default_policy: bool
        :param renewable:  Set to false to disable the ability of the token to be renewed past its initial TTL.
            Setting the value to true will allow the token to be renewable up to the system/mount maximum TTL.
        :type renewable: bool
        :param ttl: The TTL period of the token, provided as "1h", where hour is the largest suffix. If not provided,
            the token is valid for the default lease TTL, or indefinitely if the root policy is used.
        :type ttl: str
        :param type: The token type. Can be "batch" or "service". Defaults to the type
            specified by the role configuration named by role_name.
        :type type: str
        :param explicit_max_ttl: If set, the token will have an explicit max TTL set upon it.
            This maximum token TTL cannot be changed later, and unlike with normal tokens, updates to the system/mount
            max TTL value will have no effect at renewal time -- the token will never be able to be renewed or used past
            the value set at issue time.
        :type explicit_max_ttl: str
        :param display_name: The display name of the token.
        :type display_name: str
        :param num_uses: The maximum uses for the given token. This can be
            used to create a one-time-token or limited use token. The value of 0 has no
            limit to the number of uses.
        :type num_uses: int
        :param period: If specified, the token will be periodic; it will have
            no maximum TTL (unless an "explicit-max-ttl" is also set) but every renewal
            will use the given period. Requires a root token or one with the sudo capability.
        :type period: str
        :param entity_alias: Name of the entity alias to associate with during token creation.
            Only works in combination with role_name argument and used entity alias must be listed in
            `allowed_entity_aliases`. If this has been specified, the entity will not be inherited from the parent.
        :type entity_alias: str
        :param wrap_ttl: Specifies response wrapping token creation with duration. IE: '15s', '20m', '25h'.
        :type wrap_ttl: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the create request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "id": id,
                "policies": policies,
                "meta": meta,
                "no_parent": no_parent,
                "no_default_policy": no_default_policy,
                "renewable": renewable,
                "ttl": ttl,
                "type": type,
                "explicit_max_ttl": explicit_max_ttl,
                "display_name": display_name,
                "num_uses": num_uses,
                "period": period,
                "entity_alias": entity_alias,
            }
        )

        api_path = f"/v1/auth/{mount_point}/create"

        if role_name is not None:
            api_path = f"{api_path}/{role_name}"

        return self._adapter.post(
            url=api_path,
            json=params,
            wrap_ttl=wrap_ttl,
        )

    def create_orphan(
        self,
        id=None,
        role_name=None,
        policies=None,
        meta=None,
        no_default_policy=False,
        renewable=True,
        ttl=None,
        type=None,
        explicit_max_ttl=None,
        display_name="token",
        num_uses=0,
        period=None,
        entity_alias=None,
        wrap_ttl=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create a new orphaned token.

        Creates a token via the /auth/token/create-orphan endpoint. A root token
        is not required to create an orphan token with this endpoint (otherwise
        an orphaned token can be set with the `create` method's `no_parent` option).


        :param id: The ID of the client token. Can only be specified by a root token.
            The ID provided may not contain a `.` character. Otherwise, the
            token ID is a randomly generated value.
        :type id: str
        :param role_name: The name of the token role.
        :type role_name: str
        :param policies: A list of policies for the token. This must be a
            subset of the policies belonging to the token making the request, unless root.
            If not specified, defaults to all the policies of the calling token.
        :type policies: list
        :param meta: A map of string to string valued metadata. This is
            passed through to the audit devices.
        :type meta: map
        :param no_default_policy: If `True` the default policy will not be contained in this token's policy set.
        :type no_default_policy: bool
        :param renewable:  Set to false to disable the ability of the token to be renewed past its initial TTL.
            Setting the value to true will allow the token to be renewable up to the system/mount maximum TTL.
        :type renewable: bool
        :param ttl: The TTL period of the token, provided as `1h`, where hour is the largest suffix. If not provided,
            the token is valid for the default lease TTL, or indefinitely if the root policy is used.
        :type ttl: str
        :param type: The token type. Can be `batch` or `service`. Defaults to the type
            specified by the role configuration named by role_name.
        :type type: str
        :param explicit_max_ttl: If set, the token will have an explicit max TTL set upon it.
            This maximum token TTL cannot be changed later, and unlike with normal tokens, updates to the system/mount
            max TTL value will have no effect at renewal time -- the token will never be able to be renewed or used past
            the value set at issue time.
        :type explicit_max_ttl: str
        :param display_name: The display name of the token.
        :type display_name: str
        :param num_uses: The maximum uses for the given token. This can be
            used to create a one-time-token or limited use token. The value of `0` has no
            limit to the number of uses.
        :type num_uses: int
        :param period: If specified, the token will be periodic; it will have
            no maximum TTL (unless an `explicit-max-ttl` is also set) but every renewal
            will use the given period. Requires a root token or one with the sudo capability.
        :type period: str
        :param entity_alias: Name of the entity alias to associate with during token creation.
            Only works in combination with role_name argument and used entity alias must be listed in
            `allowed_entity_aliases`. If this has been specified, the entity will not be inherited from the parent.
        :type entity_alias: str
        :param wrap_ttl: Specifies response wrapping token creation with duration. IE: `15s`, `20m`, `25h`.
        :type wrap_ttl: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the create request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "id": id,
                "role_name": role_name,
                "policies": policies,
                "meta": meta,
                "no_default_policy": no_default_policy,
                "renewable": renewable,
                "ttl": ttl,
                "type": type,
                "explicit_max_ttl": explicit_max_ttl,
                "display_name": display_name,
                "num_uses": num_uses,
                "period": period,
                "entity_alias": entity_alias,
            }
        )

        api_path = f"/v1/auth/{mount_point}/create-orphan"
        return self._adapter.post(
            url=api_path,
            json=params,
            wrap_ttl=wrap_ttl,
        )

    def list_accessors(self, mount_point=DEFAULT_MOUNT_POINT):
        """List token accessors.

        This requires sudo capability, and access to it should be tightly controlled
        as the accessors can be used to revoke very large numbers of tokens and their associated leases at once.

        Supported methods:
            LIST: /auth/{mount_point}/accessors.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the list_accessors request.
        :rtype: requests.Response
        """
        api_path = f"/v1/auth/{mount_point}/accessors"
        return self._adapter.list(
            url=api_path,
        )

    def lookup(self, token, mount_point=DEFAULT_MOUNT_POINT):
        """Retrieve information about the client token.

        Supported methods:
            POST: /auth/{mount_point}/lookup.

        :param token: Token to lookup.
        :type token: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the lookup_a request.
        :rtype: requests.Response
        """
        params = {
            "token": token,
        }
        api_path = f"/v1/auth/{mount_point}/lookup"
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def lookup_self(self, mount_point=DEFAULT_MOUNT_POINT):
        """Retrieve information about the current client token.

        Supported methods:
            GET: /auth/{mount_point}/lookup-self.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the lookup_a_self request.
        :rtype: requests.Response
        """
        api_path = f"/v1/auth/{mount_point}/lookup-self"
        return self._adapter.get(
            url=api_path,
        )

    def lookup_accessor(self, accessor, mount_point=DEFAULT_MOUNT_POINT):
        """Retrieve information about the client token from its accessor.

        Supported methods:
            POST: /auth/{mount_point}/lookup-accessor.

        :param accessor: Token accessor to lookup.
        :type accessor: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the lookup_accessor request.
        :rtype: requests.Response
        """
        params = {
            "accessor": accessor,
        }
        api_path = "/v1/auth/{mount_point}/lookup-accessor".format(
            mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def renew(
        self, token, increment=None, wrap_ttl=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Renew a lease associated with a token.

        This is used to prevent the expiration of a token, and the automatic revocation of it.
        Token renewal is possible only if there is a lease associated with it.

        Supported methods:
            POST: /auth/{mount_point}/renew.

        :param token: Token to renew. This can be part of the URL  or the body.
        :type token: str
        :param increment: An optional requested lease increment can be provided.
            This increment may be ignored.
        :type increment: str
        :param wrap_ttl: Specifies response wrapping token creation with duration. IE: '15s', '20m', '25h'.
        :type wrap_ttl: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the renew_a request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "token": token,
                "increment": increment,
            }
        )
        api_path = f"/v1/auth/{mount_point}/renew"
        return self._adapter.post(
            url=api_path,
            json=params,
            wrap_ttl=wrap_ttl,
        )

    def renew_self(
        self, increment=None, wrap_ttl=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Renew a lease associated with the calling token.

        This is used to prevent the expiration of a token, and the automatic revocation of it.
        Token renewal is possible only if there is a lease associated with it.

        Supported methods:
            POST: /auth/{mount_point}/renew-self.

        :param increment: An optional requested lease increment can be
            provided. This increment may be ignored.
        :type increment: str
        :param wrap_ttl: Specifies response wrapping token creation with duration. IE: '15s', '20m', '25h'.
        :type wrap_ttl: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the renew_a_self request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "increment": increment,
            }
        )
        api_path = f"/v1/auth/{mount_point}/renew-self"
        return self._adapter.post(
            url=api_path,
            json=params,
            wrap_ttl=wrap_ttl,
        )

    def renew_accessor(
        self, accessor, increment=None, wrap_ttl=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Renew a lease associated with a token using its accessor.

        This is used to prevent the expiration of a token, and the automatic revocation of it.
        Token renewal is possible only if there is a lease associated with it.

        Supported methods:
            POST: /auth/{mount_point}/renew-accessor.

        :param accessor: Accessor associated with the token to
            renew.
        :type accessor: str
        :param increment: An optional requested lease increment can be
            provided. This increment may be ignored.
        :type increment: str
        :param wrap_ttl: Specifies response wrapping token creation with duration. IE: '15s', '20m', '25h'.
        :type wrap_ttl: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the renew_a_accessor request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "accessor": accessor,
                "increment": increment,
            }
        )
        api_path = "/v1/auth/{mount_point}/renew-accessor".format(
            mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
            wrap_ttl=wrap_ttl,
        )

    def revoke(self, token, mount_point=DEFAULT_MOUNT_POINT):
        """Revoke a token and all child tokens.

        When the token is revoked, all dynamic secrets generated with it are also revoked.

        Supported methods:
            POST: /auth/{mount_point}/revoke.

        :param token: Token to revoke.
        :type token: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the revoke_a request.
        :rtype: requests.Response
        """
        params = {
            "token": token,
        }
        api_path = f"/v1/auth/{mount_point}/revoke"
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def revoke_self(self, mount_point=DEFAULT_MOUNT_POINT):
        """Revoke the token used to call it and all child tokens.

        When the token is revoked, all dynamic secrets generated with it are also revoked.

        Supported methods:
            POST: /auth/{mount_point}/revoke-self.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the revoke_a_self request.
        :rtype: requests.Response
        """
        api_path = f"/v1/auth/{mount_point}/revoke-self"
        return self._adapter.post(url=api_path)

    def revoke_accessor(self, accessor, mount_point=DEFAULT_MOUNT_POINT):
        """Revoke the token associated with the accessor and all the child tokens.

        This is meant for purposes where there is no access to token ID but there is need to
        revoke a token and its children.

        Supported methods:
            POST: /auth/{mount_point}/revoke-accessor.

        :param accessor: Accessor of the token.
        :type accessor: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the revoke_a_accessor request.
        :rtype: requests.Response
        """
        params = {
            "accessor": accessor,
        }
        api_path = "/v1/auth/{mount_point}/revoke-accessor".format(
            mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def revoke_and_orphan_children(self, token, mount_point=DEFAULT_MOUNT_POINT):
        """Revoke a token but not its child tokens.

        When the token is revoked, all secrets generated with it are also revoked.
        All child tokens are orphaned, but can be revoked sub-sequently using /auth/token/revoke/.
        This is a root-protected endpoint.

        Supported methods:
            POST: /auth/{mount_point}/revoke-orphan.

        :param token: Token to revoke.
        :type token: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the revoke_and_orphan_children request.
        :rtype: requests.Response
        """
        params = {
            "token": token,
        }
        api_path = "/v1/auth/{mount_point}/revoke-orphan".format(
            mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_role(self, role_name, mount_point=DEFAULT_MOUNT_POINT):
        """Read the named role configuration.

        Supported methods:
            GET: /auth/{mount_point}/roles/{role_name}.

        :param role_name: The name of the token role.
        :type role_name: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the read_role request.
        :rtype: requests.Response
        """
        api_path = "/v1/auth/{mount_point}/roles/{role_name}".format(
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def list_roles(
        self,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """List available token roles.

        Supported methods:
            LIST: /auth/{mount_point}/roles.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the list_roles request.
        :rtype: requests.Response
        """
        api_path = f"/v1/auth/{mount_point}/roles"
        return self._adapter.list(
            url=api_path,
        )

    def create_or_update_role(
        self,
        role_name,
        allowed_policies=None,
        disallowed_policies=None,
        orphan=False,
        renewable=True,
        path_suffix=None,
        allowed_entity_aliases=None,
        mount_point=DEFAULT_MOUNT_POINT,
        token_period=None,
        token_explicit_max_ttl=None,
    ):
        """Create (or replace) the named role.

        Roles enforce specific behavior when creating tokens that allow token functionality that is otherwise not
        available or would require sudo/root privileges to access. Role parameters, when set, override any provided
        options to the create endpoints. The role name is also included in the token path, allowing all tokens created
        against a role to be revoked using the `/sys/leases/revoke-prefix` endpoint.

        Supported methods:
            POST: /auth/{mount_point}/roles/{role_name}.

        :param role_name: The name of the token role.
        :type role_name: str
        :param allowed_policies: will be added to the created
            token automatically.
        :type allowed_policies: list
        :param disallowed_policies: being added automatically to created
            tokens.
        :type disallowed_policies: list
        :param orphan: tokens created against this policy will
            be orphan tokens (they will have no parent). As such, they will not be
            automatically revoked by the revocation of any other token.
        :type orphan: bool
        :param renewable: allow
            the token to be renewable up to the system/mount maximum TTL.
        :type renewable: bool
        :param path_suffix:
        :type path_suffix: str
        :param allowed_entity_aliases: not case sensitive.
        :type allowed_entity_aliases: str
        :param token_period: the token will have no maximum TTL, every renewal will use the given period.
        :type token_period: str
        :param token_explicit_max_ttl: the token cannot be renewed past this TTL value.
        :type token_explicit_max_ttl: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the create_or_update_role request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "allowed_policies": allowed_policies,
                "disallowed_policies": disallowed_policies,
                "orphan": orphan,
                "renewable": renewable,
                "path_suffix": path_suffix,
                "allowed_entity_aliases": allowed_entity_aliases,
                "token_period": token_period,
                "token_explicit_max_ttl": token_explicit_max_ttl,
            }
        )
        api_path = "/v1/auth/{mount_point}/roles/{role_name}".format(
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def delete_role(self, role_name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete the named token role.

        Supported methods:
            DELETE: /auth/{mount_point}/roles/{role_name}.

        :param role_name: The name of the token role.
        :type role_name: str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the delete_role request.
        :rtype: requests.Response
        """
        api_path = "/v1/auth/{mount_point}/roles/{role_name}".format(
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def tidy(self, mount_point=DEFAULT_MOUNT_POINT):
        """Perform some maintenance tasks to clean up invalid entries that may remain in the token store.

        On Enterprise, Tidy will only impact the tokens in the specified namespace, or the root namespace if unspecified.

        Supported methods:
            POST: /auth/{mount_point}/tidy.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str
        :return: The response of the tidy_s request.
        :rtype: requests.Response
        """
        api_path = f"/v1/auth/{mount_point}/tidy"
        return self._adapter.post(
            url=api_path,
        )
