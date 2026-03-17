#!/usr/bin/env python
"""USERPASS methods module."""
from hvac import utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "userpass"


class Userpass(VaultApiBase):
    """USERPASS Auth Method (API).
    Reference: https://www.vaultproject.io/api/auth/userpass/index.html
    """

    def create_or_update_user(
        self,
        username,
        password=None,
        policies=None,
        mount_point=DEFAULT_MOUNT_POINT,
        **kwargs,
    ):
        """
        Create/update user in userpass.

        Supported methods:
            POST: /auth/{mount_point}/users/{username}. Produces: 204 (empty body)

        :param username: The username for the user.
        :type username: str | unicode
        :param password: The password for the user. Only required when creating the user.
        :type password: str | unicode
        :param policies: The list of policies to be set on username created.
        :type policies: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :param kwargs: Additional arguments to pass along with the corresponding request to Vault.
        :type kwargs: dict
        """
        params = utils.remove_nones(
            {
                "password": password,
                "policies": policies,
            }
        )
        params.update(kwargs)

        api_path = "/v1/auth/{mount_point}/users/{username}".format(
            mount_point=mount_point, username=username
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def list_user(self, mount_point=DEFAULT_MOUNT_POINT):
        """
        List existing users that have been created in the auth method

        Supported methods:
            LIST: /auth/{mount_point}/users. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the list_groups request.
        :rtype: dict
        """
        api_path = f"/v1/auth/{mount_point}/users"
        return self._adapter.list(
            url=api_path,
        )

    def read_user(self, username, mount_point=DEFAULT_MOUNT_POINT):
        """
        Read user in the auth method.

        Supported methods:
            GET: /auth/{mount_point}/users/{username}. Produces: 200 application/json

        :param username: The username for the user.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_group request.
        :rtype: dict
        """
        api_path = "/v1/auth/{mount_point}/users/{username}".format(
            mount_point=mount_point, username=username
        )
        return self._adapter.get(
            url=api_path,
        )

    def delete_user(self, username, mount_point=DEFAULT_MOUNT_POINT):
        """
        Delete user in the auth method.

        Supported methods:
            GET: /auth/{mount_point}/users/{username}. Produces: 200 application/json

        :param username: The username for the user.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the read_group request.
        :rtype: dict
        """
        api_path = "/v1/auth/{mount_point}/users/{username}".format(
            mount_point=mount_point, username=username
        )
        return self._adapter.delete(
            url=api_path,
        )

    def update_password_on_user(
        self, username, password, mount_point=DEFAULT_MOUNT_POINT
    ):
        """
        update password for the user in userpass.

        Supported methods:
            POST: /auth/{mount_point}/users/{username}/password. Produces: 204 (empty body)

        :param username: The username for the user.
        :type username: str | unicode
        :param password: The password for the user. Only required when creating the user.
        :type password: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        """
        params = {
            "password": password,
        }
        api_path = "/v1/auth/{mount_point}/users/{username}/password".format(
            mount_point=mount_point, username=username
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def login(
        self, username, password, use_token=True, mount_point=DEFAULT_MOUNT_POINT
    ):
        """
        Log in with USERPASS credentials.

        Supported methods:
            POST: /auth/{mount_point}/login/{username}. Produces: 200 application/json

        :param username: The username for the user.
        :type username: str | unicode
        :param password: The password for the user. Only required when creating the user.
        :type password: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        """
        params = {
            "password": password,
        }
        api_path = "/v1/auth/{mount_point}/login/{username}".format(
            mount_point=mount_point, username=username
        )
        return self._adapter.login(
            url=api_path,
            use_token=use_token,
            json=params,
        )
