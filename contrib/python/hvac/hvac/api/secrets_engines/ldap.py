#!/usr/bin/env python
"""LDAP methods module."""

from hvac import utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "ldap"


class Ldap(VaultApiBase):
    """LDAP Secrets Engine (API).
    Reference: https://www.vaultproject.io/api/secret/ldap/index.html
    """

    def configure(
        self,
        binddn=None,
        bindpass=None,
        url=None,
        password_policy=None,
        schema=None,
        userdn=None,
        userattr=None,
        upndomain=None,
        connection_timeout=None,
        request_timeout=None,
        starttls=None,
        insecure_tls=None,
        certificate=None,
        client_tls_cert=None,
        client_tls_key=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Configure shared information for the ldap secrets engine.

        Supported methods:
            POST: /{mount_point}/config. Produces: 204 (empty body)

        :param binddn: Distinguished name of object to bind when performing user and group search.
        :type binddn: str | unicode
        :param bindpass: Password to use along with binddn when performing user search.
        :type bindpass: str | unicode
        :param url: Base DN under which to perform user search.
        :type url: str | unicode
        :param userdn: Base DN under which to perform user search.
        :type userdn: str | unicode
        :param upndomain: userPrincipalDomain used to construct the UPN string for the authenticating user.
        :type upndomain: str | unicode
        :param password_policy: The name of the password policy to use to generate passwords.
        :type password_policy: str | unicode
        :param schema: The LDAP schema to use when storing entry passwords. Valid schemas include ``openldap``, ``ad``, and ``racf``.
        :type schema: str | unicode
        :param connection_timeout: Timeout, in seconds, when attempting to connect to the LDAP server before trying the next URL in the configuration.
        :type connection_timeout: int | str
        :param request_timeout: Timeout, in seconds, for the connection when making requests against the server before returning back an error.
        :type request_timeout: int | str
        :param starttls: If true, issues a StartTLS command after establishing an unencrypted connection.
        :type starttls: bool
        :param insecure_tls: If true, skips LDAP server SSL certificate verification - insecure, use with caution!
        :type insecure_tls: bool
        :param certificate: CA certificate to use when verifying LDAP server certificate, must be x509 PEM encoded.
        :type certificate: str | unicode
        :param client_tls_cert: Client certificate to provide to the LDAP server, must be x509 PEM encoded.
        :type client_tls_cert: str | unicode
        :param client_tls_key: Client key to provide to the LDAP server, must be x509 PEM encoded.
        :type client_tls_key: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "binddn": binddn,
                "bindpass": bindpass,
                "url": url,
                "userdn": userdn,
                "userattr": userattr,
                "upndomain": upndomain,
                "password_policy": password_policy,
                "schema": schema,
                "connection_timeout": connection_timeout,
                "request_timeout": request_timeout,
                "starttls": starttls,
                "insecure_tls": insecure_tls,
                "certificate": certificate,
                "client_tls_cert": client_tls_cert,
                "client_tls_key": client_tls_key,
            }
        )

        api_path = utils.format_url("/v1/{mount_point}/config", mount_point=mount_point)
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_config(self, mount_point=DEFAULT_MOUNT_POINT):
        """Read the configured shared information for the ldap secrets engine.

        Credentials will be omitted from returned data.

        Supported methods:
            GET: /{mount_point}/config. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url("/v1/{mount_point}/config", mount_point=mount_point)
        return self._adapter.get(
            url=api_path,
        )

    def rotate_root(self, mount_point=DEFAULT_MOUNT_POINT):
        """Rotate the root password for the binddn entry used to manage the ldap secrets engine.

        Supported methods:
            POST: /{mount_point}/rotate root. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/rotate-root", mount_point=mount_point
        )
        return self._adapter.post(url=api_path)

    def create_or_update_static_role(
        self,
        name,
        username=None,
        dn=None,
        rotation_period=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """This endpoint creates or updates the ldap static role definition.

        :param name: Specifies the name of an existing static role against which to create this ldap credential.
        :type name: str | unicode
        :param username: The name of a pre-existing service account in LDAP that maps to this static role.
            This value is required on create and cannot be updated.
        :type username: str | unicode
        :param dn: Distinguished name of the existing LDAP entry to manage password rotation for (takes precedence over username).
            Optional but cannot be modified after creation.
        :type dn: str | unicode
        :param rotation_period: How often Vault should rotate the password.
            This is provided as a string duration with a time suffix like "30s" or "1h" or as seconds.
            If not provided, the default Vault rotation_period is used.
        :type rotation_period: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/static-role/{}", mount_point, name)
        params = {"username": username, "rotation_period": rotation_period}
        params.update(utils.remove_nones({"dn": dn}))
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_static_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint queries for information about an ldap static role with the given name.
        If no role exists with that name, a 404 is returned.
        :param name: Specifies the name of the static role to query.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/static-role/{}", mount_point, name)
        return self._adapter.get(
            url=api_path,
        )

    def list_static_roles(self, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint lists all existing static roles in the secrets engine.
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/static-role", mount_point)
        return self._adapter.list(
            url=api_path,
        )

    def delete_static_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint deletes an ldap static role with the given name.
        Even if the role does not exist, this endpoint will still return a successful response.
        :param name: Specifies the name of the role to delete.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/static-role/{}", mount_point, name)
        return self._adapter.delete(
            url=api_path,
        )

    def generate_static_credentials(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint retrieves the previous and current LDAP password for
        the associated account (or rotate if required)

        :param name: Specifies the name of the static role to request credentials from.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/static-cred/{}", mount_point, name)
        return self._adapter.get(
            url=api_path,
        )

    def rotate_static_credentials(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint rotates the password of an existing static role.

        :param name: Specifies the name of the static role to rotate credentials for.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/rotate-role/{}", mount_point, name)
        return self._adapter.post(
            url=api_path,
        )
