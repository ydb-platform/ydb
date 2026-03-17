#!/usr/bin/env python
"""Okta methods module."""
from hvac import utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "okta"


class Okta(VaultApiBase):
    """Okta Auth Method (API).

    Reference: https://www.vaultproject.io/api/auth/okta/index.html
    """

    def configure(
        self,
        org_name,
        api_token=None,
        base_url=None,
        ttl=None,
        max_ttl=None,
        bypass_okta_mfa=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Configure the connection parameters for Okta.

        This path honors the distinction between the create and update capabilities inside ACL policies.

        Supported methods:
            POST: /auth/{mount_point}/config. Produces: 204 (empty body)


        :param org_name: Name of the organization to be used in the Okta API.
        :type org_name: str | unicode
        :param api_token: Okta API token. This is required to query Okta for user group membership. If this is not
            supplied only locally configured groups will be enabled.
        :type api_token: str | unicode
        :param base_url:  If set, will be used as the base domain for API requests.  Examples are okta.com,
            oktapreview.com, and okta-emea.com.
        :type base_url: str | unicode
        :param ttl: Duration after which authentication will be expired.
        :type ttl: str | unicode
        :param max_ttl: Maximum duration after which authentication will be expired.
        :type max_ttl: str | unicode
        :param bypass_okta_mfa: Whether to bypass an Okta MFA request. Useful if using one of Vault's built-in MFA
            mechanisms, but this will also cause certain other statuses to be ignored, such as PASSWORD_EXPIRED.
        :type bypass_okta_mfa: bool
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "org_name": org_name,
        }
        params.update(
            utils.remove_nones(
                {
                    "api_token": api_token,
                    "base_url": base_url,
                    "ttl": ttl,
                    "max_ttl": max_ttl,
                    "bypass_okta_mfa": bypass_okta_mfa,
                }
            )
        )
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_config(self, mount_point=DEFAULT_MOUNT_POINT):
        """Read the Okta configuration.

        Supported methods:
            GET: /auth/{mount_point}/config. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config", mount_point=mount_point
        )
        return self._adapter.get(
            url=api_path,
        )

    def list_users(self, mount_point=DEFAULT_MOUNT_POINT):
        """List the users configured in the Okta method.

        Supported methods:
            LIST: /auth/{mount_point}/users. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/users", mount_point=mount_point
        )
        return self._adapter.list(
            url=api_path,
        )

    def register_user(
        self, username, groups=None, policies=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Register a new user and maps a set of policies to it.

        Supported methods:
            POST: /auth/{mount_point}/users/{username}. Produces: 204 (empty body)

        :param username: Name of the user.
        :type username: str | unicode
        :param groups: List or comma-separated string of groups associated with the user.
        :type groups: list
        :param policies: List or comma-separated string of policies associated with the user.
        :type policies: list
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "username": username,
        }
        params.update(
            utils.remove_nones(
                {
                    "groups": groups,
                    "policies": policies,
                }
            )
        )
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/users/{username}",
            mount_point=mount_point,
            username=username,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_user(self, username, mount_point=DEFAULT_MOUNT_POINT):
        """Read the properties of an existing username.

        Supported methods:
            GET: /auth/{mount_point}/users/{username}. Produces: 200 application/json

        :param username: Username for this user.
        :type username: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        params = {
            "username": username,
        }
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/users/{username}",
            mount_point=mount_point,
            username=username,
        )
        return self._adapter.get(
            url=api_path,
            json=params,
        )

    def delete_user(self, username, mount_point=DEFAULT_MOUNT_POINT):
        """Delete an existing username from the method.

        Supported methods:
            DELETE: /auth/{mount_point}/users/{username}. Produces: 204 (empty body)

        :param username: Username for this user.
        :type username: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "username": username,
        }
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/users/{username}",
            mount_point=mount_point,
            username=username,
        )
        return self._adapter.delete(
            url=api_path,
            json=params,
        )

    def list_groups(self, mount_point=DEFAULT_MOUNT_POINT):
        """List the groups configured in the Okta method.

        Supported methods:
            LIST: /auth/{mount_point}/groups. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/groups", mount_point=mount_point
        )
        return self._adapter.list(
            url=api_path,
        )

    def register_group(self, name, policies=None, mount_point=DEFAULT_MOUNT_POINT):
        """Register a new group and maps a set of policies to it.

        Supported methods:
            POST: /auth/{mount_point}/groups/{name}. Produces: 204 (empty body)

        :param name: The name of the group.
        :type name: str | unicode
        :param policies: The list or comma-separated string of policies associated with the group.
        :type policies: list
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "policies": policies,
            }
        )
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/groups/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_group(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Read the properties of an existing group.

        Supported methods:
            GET: /auth/{mount_point}/groups/{name}. Produces: 200 application/json

        :param name: The name for the group.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/groups/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def delete_group(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete an existing group from the method.

        Supported methods:
            DELETE: /auth/{mount_point}/groups/{name}. Produces: 204 (empty body)

        :param name: The name for the group.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "name": name,
        }
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/groups/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.delete(
            url=api_path,
            json=params,
        )

    def login(
        self, username, password, use_token=True, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Login with the username and password.

        Supported methods:
            POST: /auth/{mount_point}/login/{username}. Produces: 200 application/json

        :param username: Username for this user.
        :type username: str | unicode
        :param password: Password for the authenticating user.
        :type password: str | unicode
        :param use_token: if True, uses the token in the response received from the auth request to set the "token"
            attribute on the :py:meth:`hvac.adapters.Adapter` instance under the _adapter Client attribute.
        :type use_token: bool
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the login request.
        :rtype: dict
        """
        params = {
            "username": username,
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
