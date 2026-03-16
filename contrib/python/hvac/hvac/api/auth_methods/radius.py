#!/usr/bin/env python
"""RADIUS methods module."""
from hvac import exceptions, utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "radius"


class Radius(VaultApiBase):
    """RADIUS Auth Method (API).

    Reference: https://www.vaultproject.io/docs/auth/radius.html
    """

    def configure(
        self,
        host,
        secret,
        port=None,
        unregistered_user_policies=None,
        dial_timeout=None,
        nas_port=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """
        Configure the RADIUS auth method.

        Supported methods:
            POST: /auth/{mount_point}/config. Produces: 204 (empty body)

        :param host: The RADIUS server to connect to. Examples: radius.myorg.com, 127.0.0.1
        :type host: str | unicode
        :param secret: The RADIUS shared secret.
        :type secret: str | unicode
        :param port: The UDP port where the RADIUS server is listening on. Defaults is 1812.
        :type port: int
        :param unregistered_user_policies: A comma-separated list of policies to be granted to unregistered users.
        :type unregistered_user_policies: list
        :param dial_timeout: Number of second to wait for a backend connection before timing out. Default is 10.
        :type dial_timeout: int
        :param nas_port: The NAS-Port attribute of the RADIUS request. Defaults is 10.
        :type nas_port: int
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the configure request.
        :rtype: requests.Response
        """
        params = {
            "host": host,
            "secret": secret,
        }
        params.update(
            utils.remove_nones(
                {
                    "port": port,
                    "dial_timeout": dial_timeout,
                    "nas_port": nas_port,
                }
            )
        )
        # Fill out params dictionary with any optional parameters provided
        if unregistered_user_policies is not None:
            if not isinstance(unregistered_user_policies, list):
                error_msg = (
                    '"unregistered_user_policies" argument must be an instance of list or None, '
                    '"{unregistered_user_policies}" provided.'
                ).format(unregistered_user_policies=type(unregistered_user_policies))
                raise exceptions.ParamValidationError(error_msg)

            params["unregistered_user_policies"] = ",".join(unregistered_user_policies)

        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_configuration(self, mount_point=DEFAULT_MOUNT_POINT):
        """
        Retrieve the RADIUS configuration for the auth method.

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

    def register_user(self, username, policies=None, mount_point=DEFAULT_MOUNT_POINT):
        """
        Create or update RADIUS user with a set of policies.

        Supported methods:
            POST: /auth/{mount_point}/users/{username}. Produces: 204 (empty body)

        :param username: Username for this RADIUS user.
        :type username: str | unicode
        :param policies: List of policies associated with the user. This parameter is transformed to a comma-delimited
            string before being passed to Vault.
        :type policies: list
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the register_user request.
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
            "/v1/auth/{mount_point}/users/{name}",
            mount_point=mount_point,
            name=username,
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
        Read policies associated with a RADIUS user.

        Supported methods:
            GET: /auth/{mount_point}/users/{username}. Produces: 200 application/json


        :param username: The username of the RADIUS user
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
        Delete a RADIUS user and policy association.

        Supported methods:
            DELETE: /auth/{mount_point}/users/{username}. Produces: 204 (empty body)


        :param username: The username of the RADIUS user
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
        Log in with RADIUS credentials.

        Supported methods:
            POST: /auth/{mount_point}/login/{username}. Produces: 200 application/json


        :param username: The username of the RADIUS user
        :type username: str | unicode
        :param password: The password for the RADIUS user
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
