#!/usr/bin/env python
"""APPROLE methods module."""
import json
from hvac import exceptions, utils
from hvac.api.vault_api_base import VaultApiBase
from hvac.constants.approle import DEFAULT_MOUNT_POINT, ALLOWED_TOKEN_TYPES
from hvac.utils import validate_list_of_strings_param, list_to_comma_delimited


class AppRole(VaultApiBase):
    """USERPASS Auth Method (API).
    Reference: https://www.vaultproject.io/api-docs/auth/approle/index.html
    """

    def create_or_update_approle(
        self,
        role_name,
        bind_secret_id=None,
        secret_id_bound_cidrs=None,
        secret_id_num_uses=None,
        secret_id_ttl=None,
        enable_local_secret_ids=None,
        token_ttl=None,
        token_max_ttl=None,
        token_policies=None,
        token_bound_cidrs=None,
        token_explicit_max_ttl=None,
        token_no_default_policy=None,
        token_num_uses=None,
        token_period=None,
        token_type=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """
        Create/update approle.

        Supported methods:
            POST: /auth/{mount_point}/role/{role_name}. Produces: 204 (empty body)

        :param role_name: The name for the approle.
        :type role_name: str | unicode
        :param bind_secret_id: Require secret_id to be presented when logging in using this approle.
        :type bind_secret_id: bool
        :param secret_id_bound_cidrs: Blocks of IP addresses which can perform login operations.
        :type secret_id_bound_cidrs: list
        :param secret_id_num_uses: Number of times any secret_id can be used to fetch a token.
            A value of zero allows unlimited uses.
        :type secret_id_num_uses: int
        :param secret_id_ttl: Duration after which a secret_id expires. This can be specified
            as an integer number of seconds or as a duration value like "5m".
        :type secret_id_ttl: str | unicode
        :param enable_local_secret_ids: Secret IDs generated using role will be cluster local.
        :type enable_local_secret_ids: bool
        :param token_ttl: Incremental lifetime for generated tokens. This can be specified
            as an integer number of seconds or as a duration value like "5m".
        :type token_ttl: str | unicode
        :param token_max_ttl: Maximum lifetime for generated tokens: This can be specified
            as an integer number of seconds or as a duration value like "5m".
        :type token_max_ttl: str | unicode
        :param token_policies: List of policies to encode onto generated tokens.
        :type token_policies: list
        :param token_bound_cidrs: Blocks of IP addresses which can authenticate successfully.
        :type token_bound_cidrs: list
        :param token_explicit_max_ttl: If set, will encode an explicit max TTL onto the token. This can be specified
            as an integer number of seconds or as a duration value like "5m".
        :type token_explicit_max_ttl: str | unicode
        :param token_no_default_policy: Do not add the default policy to generated tokens, use only tokens
            specified in token_policies.
        :type token_no_default_policy: bool
        :param token_num_uses: Maximum number of times a generated token may be used. A value of zero
            allows unlimited uses.
        :type token_num_uses: int
        :param token_period: The period, if any, to set on the token. This can be specified
            as an integer number of seconds or as a duration value like "5m".
        :type token_period: str | unicode
        :param token_type: The type of token that should be generated, can be "service", "batch", or "default".
        :type token_type: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        """
        list_of_strings_params = {
            "secret_id_bound_cidrs": secret_id_bound_cidrs,
            "token_policies": token_policies,
            "token_bound_cidrs": token_bound_cidrs,
        }

        if token_type is not None and token_type not in ALLOWED_TOKEN_TYPES:
            error_msg = 'unsupported token_type argument provided "{arg}", supported types: "{token_types}"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=token_type,
                    token_types=",".join(ALLOWED_TOKEN_TYPES),
                )
            )

        params = dict()

        for param_name, param_argument in list_of_strings_params.items():
            validate_list_of_strings_param(
                param_name=param_name,
                param_argument=param_argument,
            )
            if param_argument is not None:
                params[param_name] = list_to_comma_delimited(param_argument)

        params.update(
            utils.remove_nones(
                {
                    "bind_secret_id": bind_secret_id,
                    "secret_id_num_uses": secret_id_num_uses,
                    "secret_id_ttl": secret_id_ttl,
                    "enable_local_secret_ids": enable_local_secret_ids,
                    "token_ttl": token_ttl,
                    "token_max_ttl": token_max_ttl,
                    "token_explicit_max_ttl": token_explicit_max_ttl,
                    "token_no_default_policy": token_no_default_policy,
                    "token_num_uses": token_num_uses,
                    "token_period": token_period,
                    "token_type": token_type,
                }
            )
        )

        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{name}",
            mount_point=mount_point,
            name=role_name,
        )
        return self._adapter.post(url=api_path, json=params)

    def list_roles(self, mount_point=DEFAULT_MOUNT_POINT):
        """
        List existing roles created in the auth method.

        Supported methods:
            LIST: /auth/{mount_point}/role. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the list_roles request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role", mount_point=mount_point
        )
        return self._adapter.list(url=api_path)

    def read_role(self, role_name, mount_point=DEFAULT_MOUNT_POINT):
        """
        Read role in the auth method.

        Supported methods:
            GET: /auth/{mount_point}/role/{role_name}. Produces: 200 application/json

        :param role_name: The name for the role.
        :type role_name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_role request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.get(url=api_path)

    def delete_role(self, role_name, mount_point=DEFAULT_MOUNT_POINT):
        """
        Delete role in the auth method.

        Supported methods:
            DELETE: /auth/{mount_point}/role/{role_name}. Produces: 204 (empty body)

        :param role_name: The name for the role.
        :type role_name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.delete(url=api_path)

    def read_role_id(self, role_name, mount_point=DEFAULT_MOUNT_POINT):
        """
        Reads the Role ID of a role in the auth method.

        Supported methods:
            GET: /auth/{mount_point}/role/{role_name}/role-id. Produces: 200 application/json

        :param role_name: The name for the role.
        :type role_name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_role_id request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/role-id",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.get(url=api_path)

    def update_role_id(self, role_name, role_id, mount_point=DEFAULT_MOUNT_POINT):
        """
        Updates the Role ID of a role in the auth method.

        Supported methods:
            POST: /auth/{mount_point}/role/{role_name}/role-id. Produces: 200 application/json

        :param role_name: The name for the role.
        :type role_name: str | unicode
        :param role_id: New value for the Role ID.
        :type role_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_role_id request.
        :rtype: dict
        """
        params = {"role_id": role_id}

        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/role-id",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.post(url=api_path, json=params)

    def generate_secret_id(
        self,
        role_name,
        metadata=None,
        cidr_list=None,
        token_bound_cidrs=None,
        mount_point=DEFAULT_MOUNT_POINT,
        wrap_ttl=None,
    ):
        """
        Generates and issues a new Secret ID on a role in the auth method.

        Supported methods:
            POST: /auth/{mount_point}/role/{role_name}/secret-id. Produces: 200 application/json

        :param role_name: The name for the role.
        :type role_name: str | unicode
        :param metadata: Metadata to be tied to the Secret ID.
        :type metadata: dict
        :param cidr_list: Blocks of IP addresses which can perform login operations.
        :type cidr_list: list
        :param token_bound_cidrs: Blocks of IP addresses which can authenticate successfully.
        :type token_bound_cidrs: list
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :param wrap_ttl: Returns the request as a response-wrapping token.
            Can be either an integer number of seconds or a string duration of seconds (`15s`), minutes (`20m`), or hours (`25h`).
        :type wrap_ttl: int | str
        :return: The JSON response of the read_role_id request.
        :rtype: dict
        """
        if metadata is not None and not isinstance(metadata, dict):
            error_msg = 'unsupported metadata argument provided "{arg}" ({arg_type}), required type: dict"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=metadata,
                    arg_type=type(metadata),
                )
            )

        params = {}
        if metadata:
            params = {"metadata": json.dumps(metadata)}

        list_of_strings_params = {
            "cidr_list": cidr_list,
            "token_bound_cidrs": token_bound_cidrs,
        }
        for param_name, param_argument in list_of_strings_params.items():
            validate_list_of_strings_param(
                param_name=param_name,
                param_argument=param_argument,
            )
            if param_argument is not None:
                params[param_name] = list_to_comma_delimited(param_argument)

        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/secret-id",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.post(url=api_path, json=params, wrap_ttl=wrap_ttl)

    def create_custom_secret_id(
        self,
        role_name,
        secret_id,
        metadata=None,
        cidr_list=None,
        token_bound_cidrs=None,
        mount_point=DEFAULT_MOUNT_POINT,
        wrap_ttl=None,
    ):
        """
        Generates and issues a new Secret ID on a role in the auth method.

        Supported methods:
            POST: /auth/{mount_point}/role/{role_name}/custom-secret-id. Produces: 200 application/json

        :param role_name: The name for the role.
        :type role_name: str | unicode
        :param secret_id: The Secret ID to read.
        :type secret_id: str | unicode
        :param metadata: Metadata to be tied to the Secret ID.
        :type metadata: dict
        :param cidr_list: Blocks of IP addresses which can perform login operations.
        :type cidr_list: list
        :param token_bound_cidrs: Blocks of IP addresses which can authenticate successfully.
        :type token_bound_cidrs: list
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :param wrap_ttl: Returns the request as a response-wrapping token.
            Can be either an integer number of seconds or a string duration of seconds (`15s`), minutes (`20m`), or hours (`25h`).
        :type wrap_ttl: int | str
        :return: The JSON response of the read_role_id request.
        :rtype: dict
        """
        if metadata is not None and not isinstance(metadata, dict):
            error_msg = 'unsupported metadata argument provided "{arg}" ({arg_type}), required type: dict"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=metadata,
                    arg_type=type(metadata),
                )
            )

        params = {"secret_id": secret_id}

        if metadata:
            params["metadata"] = json.dumps(metadata)

        list_of_strings_params = {
            "cidr_list": cidr_list,
            "token_bound_cidrs": token_bound_cidrs,
        }
        for param_name, param_argument in list_of_strings_params.items():
            validate_list_of_strings_param(
                param_name=param_name,
                param_argument=param_argument,
            )
            if param_argument is not None:
                params[param_name] = list_to_comma_delimited(param_argument)

        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/custom-secret-id",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.post(url=api_path, json=params, wrap_ttl=wrap_ttl)

    def read_secret_id(self, role_name, secret_id, mount_point=DEFAULT_MOUNT_POINT):
        """
        Read the properties of a Secret ID for a role in the auth method.

        Supported methods:
            POST: /auth/{mount_point}/role/{role_name}/secret-id/lookup. Produces: 200 application/json

        :param role_name: The name for the role
        :type role_name: str | unicode
        :param secret_id: The Secret ID to read.
        :type secret_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_role_id request.
        :rtype: dict
        """
        params = {"secret_id": secret_id}
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/secret-id/lookup",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.post(url=api_path, json=params)

    def destroy_secret_id(self, role_name, secret_id, mount_point=DEFAULT_MOUNT_POINT):
        """
        Destroys a Secret ID for a role in the auth method.

        Supported methods:
            POST: /auth/{mount_point}/role/{role_name}/secret-id/destroy. Produces 204 (empty body)

        :param role_name: The name for the role
        :type role_name: str | unicode
        :param secret_id: The Secret ID to read.
        :type secret_id: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        """
        params = {"secret_id": secret_id}
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/secret-id/destroy",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.post(url=api_path, json=params)

    def list_secret_id_accessors(self, role_name, mount_point=DEFAULT_MOUNT_POINT):
        """
        Lists accessors of all issued Secret IDs for a role in the auth method.

        Supported methods:
            LIST: /auth/{mount_point}/role/{role_name}/secret-id. Produces: 200 application/json

        :param role_name: The name for the role
        :type role_name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_role_id request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/secret-id",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.list(url=api_path)

    def read_secret_id_accessor(
        self, role_name, secret_id_accessor, mount_point=DEFAULT_MOUNT_POINT
    ):
        """
        Read the properties of a Secret ID for a role in the auth method.

        Supported methods:
            POST: /auth/{mount_point}/role/{role_name}/secret-id-accessor/lookup. Produces: 200 application/json

        :param role_name: The name for the role
        :type role_name: str | unicode
        :param secret_id_accessor: The accessor for the Secret ID to read.
        :type secret_id_accessor: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_role_id request.
        :rtype: dict
        """
        params = {"secret_id_accessor": secret_id_accessor}
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/secret-id-accessor/lookup",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.post(url=api_path, json=params)

    def destroy_secret_id_accessor(
        self, role_name, secret_id_accessor, mount_point=DEFAULT_MOUNT_POINT
    ):
        """
        Destroys a Secret ID for a role in the auth method.

        Supported methods:
            POST: /auth/{mount_point}/role/{role_name}/secret-id-accessor/destroy. Produces: 204 (empty body)

        :param role_name: The name for the role
        :type role_name: str | unicode
        :param secret_id_accessor: The accessor for the Secret ID to read.
        :type secret_id_accessor: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        """
        params = {"secret_id_accessor": secret_id_accessor}
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{role_name}/secret-id-accessor/destroy",
            mount_point=mount_point,
            role_name=role_name,
        )
        return self._adapter.post(url=api_path, json=params)

    def login(
        self, role_id, secret_id=None, use_token=True, mount_point=DEFAULT_MOUNT_POINT
    ):
        """
        Login with APPROLE credentials.

        Supported methods:
            POST: /auth/{mount_point}/login. Produces: 200 application/json

        :param role_id: Role ID of the role.
        :type role_id: str | unicode
        :param secret_id: Secret ID of the role.
        :type secret_id: str | unicode
        :param use_token: if True, uses the token in the response received from the auth request to set the "token"
            attribute on the the :py:meth:`hvac.adapters.Adapter` instance under the _adapter Client attribute.
        :type use_token: bool
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the login request.
        :rtype: dict
        """
        params = {"role_id": role_id, "secret_id": secret_id}
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/login", mount_point=mount_point
        )
        return self._adapter.login(
            url=api_path,
            use_token=use_token,
            json=params,
        )
