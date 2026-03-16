#!/usr/bin/env python
"""Kubernetes methods module."""
from hvac import utils
from hvac.api.vault_api_base import VaultApiBase
from hvac.utils import (
    validate_list_of_strings_param,
    comma_delimited_to_list,
    validate_pem_format,
)

DEFAULT_MOUNT_POINT = "kubernetes"


class Kubernetes(VaultApiBase):
    """Kubernetes Auth Method (API).

    Reference: https://www.vaultproject.io/api/auth/kubernetes/index.html
    """

    def configure(
        self,
        kubernetes_host,
        kubernetes_ca_cert=None,
        token_reviewer_jwt=None,
        pem_keys=None,
        issuer=None,
        mount_point=DEFAULT_MOUNT_POINT,
        disable_local_ca_jwt=False,
    ):
        """Configure the connection parameters for Kubernetes.

        This path honors the distinction between the create and update capabilities inside ACL policies.

        Supported methods:
            POST: /auth/{mount_point}/config. Produces: 204 (empty body)

        :param kubernetes_host: Host must be a host string, a host:port pair, or a URL to the base of the
            Kubernetes API server. Example: https://k8s.example.com:443
        :type kubernetes_host: str | unicode
        :param kubernetes_ca_cert: PEM encoded CA cert for use by the TLS client used to talk with the Kubernetes API.
            NOTE: Every line must end with a newline: \n
        :type kubernetes_ca_cert: str | unicode
        :param token_reviewer_jwt: A service account JWT used to access the TokenReview API to validate other
            JWTs during login. If not set the JWT used for login will be used to access the API.
        :type token_reviewer_jwt: str | unicode
        :param pem_keys: Optional list of PEM-formatted public keys or certificates used to verify the signatures of
            Kubernetes service account JWTs. If a certificate is given, its public key will be extracted. Not every
            installation of Kubernetes exposes these keys.
        :type pem_keys: list
        :param issuer: Optional JWT issuer.
        :type token_reviewer_jwt: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :param disable_local_ca_jwt: Disable defaulting to the local CA cert and service account JWT
        :type disable_local_ca_jwt: bool
        :return: The response of the configure_method request.
        :rtype: requests.Response
        """
        list_of_pem_params = {
            "kubernetes_ca_cert": kubernetes_ca_cert,
            "pem_keys": pem_keys,
        }
        for param_name, param_argument in list_of_pem_params.items():
            if param_argument is not None:
                validate_pem_format(
                    param_name=param_name,
                    param_argument=param_argument,
                )

        params = {
            "kubernetes_host": kubernetes_host,
            "disable_local_ca_jwt": disable_local_ca_jwt,
        }
        params.update(
            utils.remove_nones(
                {
                    "kubernetes_ca_cert": kubernetes_ca_cert,
                    "token_reviewer_jwt": token_reviewer_jwt,
                    "pem_keys": pem_keys,
                    "issuer": issuer,
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
        """Return the previously configured config, including credentials.

        Supported methods:
            GET: /auth/{mount_point}/config. Produces: 200 application/json

        :param mount_point: The "path" the kubernetes auth method was mounted on.
        :type mount_point: str | unicode
        :return: The data key from the JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/config", mount_point=mount_point
        )
        response = self._adapter.get(
            url=api_path,
        )
        return response.get("data")

    def create_role(
        self,
        name,
        bound_service_account_names,
        bound_service_account_namespaces,
        ttl=None,
        max_ttl=None,
        period=None,
        policies=None,
        token_type="",
        mount_point=DEFAULT_MOUNT_POINT,
        alias_name_source=None,
        audience=None,
    ):
        """Create a role in the method.

        Registers a role in the auth method. Role types have specific entities that can perform login operations
        against this endpoint. Constraints specific to the role type must be set on the role. These are applied to
        the authenticated entities attempting to login.

        Supported methods:
            POST: /auth/{mount_point}/role/{name}. Produces: 204 (empty body)

        :param name: Name of the role.
        :type name: str | unicode
        :param bound_service_account_names: List of service account names able to access this role. If set to "*"
            all names are allowed.
        :type bound_service_account_names: list | str | unicode
        :param bound_service_account_namespaces: List of namespaces allowed to access this role. If set to "*" all
            namespaces are allowed.
        :type bound_service_account_namespaces: list | str | unicode
        :param ttl: The TTL period of tokens issued using this role in seconds.
        :type ttl: str | unicode
        :param max_ttl: The maximum allowed lifetime of tokens issued in seconds using this role.
        :type max_ttl: str | unicode
        :param period: If set, indicates that the token generated using this role should never expire. The token should
            be renewed within the duration specified by this value. At each renewal, the token's TTL will be set to the
            value of this parameter.
        :type period: str | unicode
        :param policies: Policies to be set on tokens issued using this role.
        :type policies: list | str | unicode
        :param token_type: The type of token that should be generated. Can be service, batch, or default to use the
            mount's tuned default (which unless changed will be service tokens). For token store roles, there are two
            additional possibilities: default-service and default-batch which specify the type to return unless the
            client requests a different type at generation time.
        :type token_type: str
        :param mount_point: The "path" the kubernetes auth method was mounted on.
        :type mount_point: str | unicode
        :param alias_name_source: Configures how identity aliases are generated.
            Valid choices are: serviceaccount_uid, serviceaccount_name.
        :type alias_name_source: str | unicode
        :param audience: Audience claim to verify in the JWT. Required in Vault 1.21+.
        :type audience: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        list_of_strings_params = {
            "bound_service_account_names": bound_service_account_names,
            "bound_service_account_namespaces": bound_service_account_namespaces,
            "policies": policies,
        }
        for param_name, param_argument in list_of_strings_params.items():
            validate_list_of_strings_param(
                param_name=param_name,
                param_argument=param_argument,
            )

        params = {
            "bound_service_account_names": comma_delimited_to_list(
                bound_service_account_names
            ),
            "bound_service_account_namespaces": comma_delimited_to_list(
                bound_service_account_namespaces
            ),
        }
        if alias_name_source is not None:
            params["alias_name_source"] = alias_name_source
        if audience is not None:
            params["audience"] = audience

        params.update(
            utils.remove_nones(
                {
                    "ttl": ttl,
                    "max_ttl": max_ttl,
                    "period": period,
                }
            )
        )
        if policies is not None:
            params["policies"] = comma_delimited_to_list(policies)

        if token_type:
            params["token_type"] = token_type

        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Returns the previously registered role configuration.

        Supported methods:
            POST: /auth/{mount_point}/role/{name}. Produces: 200 application/json

        :param name: Name of the role.
        :type name: str | unicode
        :param mount_point: The "path" the kubernetes auth method was mounted on.
        :type mount_point: str | unicode
        :return: The "data" key from the JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{name}",
            mount_point=mount_point,
            name=name,
        )
        response = self._adapter.get(
            url=api_path,
        )
        return response.get("data")

    def list_roles(self, mount_point=DEFAULT_MOUNT_POINT):
        """List all the roles that are registered with the plugin.

        Supported methods:
            LIST: /auth/{mount_point}/role. Produces: 200 application/json

        :param mount_point: The "path" the kubernetes auth method was mounted on.
        :type mount_point: str | unicode
        :return: The "data" key from the JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role", mount_point=mount_point
        )
        response = self._adapter.list(
            url=api_path,
        )
        return response.get("data")

    def delete_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete the previously registered role.

        Supported methods:
            DELETE: /auth/{mount_point}/role/{name}. Produces: 204 (empty body)


        :param name: Name of the role.
        :type name: str | unicode
        :param mount_point: The "path" the kubernetes auth method was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/auth/{mount_point}/role/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def login(self, role, jwt, use_token=True, mount_point=DEFAULT_MOUNT_POINT):
        """Fetch a token.

        This endpoint takes a signed JSON Web Token (JWT) and a role name for some entity. It verifies the JWT signature
        to authenticate that entity and then authorizes the entity for the given role.

        Supported methods:
            POST: /auth/{mount_point}/login. Produces: 200 application/json

        :param role: Name of the role against which the login is being attempted.
        :type role: str | unicode
        :param jwt: Signed JSON Web Token (JWT) from Kubernetes service account.
        :type jwt: str | unicode
        :param use_token: if True, uses the token in the response received from the auth request to set the "token"
            attribute on the the :py:meth:`hvac.adapters.Adapter` instance under the _adapter Client attribute.
        :type use_token: bool
        :param mount_point: The "path" the kubernetes auth method was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        params = {
            "role": role,
            "jwt": jwt,
        }

        api_path = utils.format_url(
            "/v1/auth/{mount_point}/login", mount_point=mount_point
        )
        response = self._adapter.login(
            url=api_path,
            use_token=use_token,
            json=params,
        )
        return response
