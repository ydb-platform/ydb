#!/usr/bin/env python
"""Gcp methods module."""
import json
import logging

from hvac import exceptions, utils
from hvac.api.vault_api_base import VaultApiBase
from hvac.constants.gcp import (
    ALLOWED_SECRETS_TYPES,
    SERVICE_ACCOUNT_KEY_ALGORITHMS,
    SERVICE_ACCOUNT_KEY_TYPES,
)

DEFAULT_MOUNT_POINT = "gcp"


class Gcp(VaultApiBase):
    """Google Cloud Secrets Engine (API).

    Reference: https://www.vaultproject.io/api/secret/gcp/index.html
    """

    def configure(
        self, credentials=None, ttl=None, max_ttl=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Configure shared information for the Gcp secrets engine.

        Supported methods:
            POST: /{mount_point}/config. Produces: 204 (empty body)

        :param credentials: JSON credentials (either file contents or '@path/to/file') See docs for alternative ways to
            pass in to this parameter, as well as the required permissions.
        :type credentials: str | unicode
        :param ttl: – Specifies default config TTL for long-lived credentials (i.e. service account keys). Accepts
            integer number of seconds or Go duration format string.
        :type ttl: int | str
        :param max_ttl: Specifies the maximum config TTL for long-lived credentials (i.e. service account keys). Accepts
            integer number of seconds or Go duration format string.**
        :type max_ttl: int | str
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = utils.remove_nones(
            {
                "credentials": credentials,
                "ttl": ttl,
                "max_ttl": max_ttl,
            }
        )
        api_path = utils.format_url("/v1/{mount_point}/config", mount_point=mount_point)
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def rotate_root_credentials(self, mount_point=DEFAULT_MOUNT_POINT):
        """Rotate the GCP service account credentials used by Vault for this mount.

        A new key will be generated for the service account, replacing the internal value, and then a deletion of the
        old service account key is scheduled. Note that this does not create a new service account, only a new version
        of the service account key.

        Supported methods:
            POST: /{mount_point}/config/rotate-root. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/config/rotate-root",
            mount_point=mount_point,
        )
        return self._adapter.post(
            url=api_path,
        )

    def read_config(self, mount_point=DEFAULT_MOUNT_POINT):
        """Read the configured shared information for the Gcp secrets engine.

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

    def create_or_update_roleset(
        self,
        name,
        project,
        bindings,
        secret_type=None,
        token_scopes=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create a roleset or update an existing roleset.

        See roleset docs for the GCP secrets backend to learn more about what happens when you create or update a
        roleset.

        Supported methods:
            POST: /{mount_point}/roleset/{name}. Produces: 204 (empty body)

        :param name: Name of the role. Cannot be updated.
        :type name: str | unicode
        :param project: Name of the GCP project that this roleset's service account will belong to. Cannot be updated.
        :type project: str | unicode
        :param bindings: Bindings configuration string (expects HCL or JSON format in raw or base64-encoded string)
        :type bindings: str | unicode
        :param secret_type: Cannot be updated.
        :type secret_type: str | unicode
        :param token_scopes: List of OAuth scopes to assign to access_token secrets generated under this role set
            (access_token role sets only)
        :type token_scopes: list[str]
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        if secret_type is not None and secret_type not in ALLOWED_SECRETS_TYPES:
            error_msg = 'unsupported secret_type argument provided "{arg}", supported types: "{secret_type}"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=secret_type,
                    secret_type=",".join(ALLOWED_SECRETS_TYPES),
                )
            )

        if isinstance(bindings, dict):
            bindings = json.dumps(bindings).replace(" ", "")
            logging.debug("bindings: %s" % bindings)

        params = {
            "project": project,
            "bindings": bindings,
        }
        params.update(
            utils.remove_nones(
                {
                    "secret_type": secret_type,
                    "token_scopes": token_scopes,
                }
            )
        )

        api_path = utils.format_url(
            "/v1/{mount_point}/roleset/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def rotate_roleset_account(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Rotate the service account this roleset uses to generate secrets.

        This also replaces the key access_token roleset. This can be used to invalidate old secrets generated by the
        roleset or fix issues if a roleset's service account (and/or keys) was changed outside of Vault (i.e.
        through GCP APIs/cloud console).

        Supported methods:
            POST: /{mount_point}/roleset/{name}/rotate. Produces: 204 (empty body)

        :param name: Name of the role.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/roleset/{name}/rotate",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
        )

    def rotate_roleset_account_key(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Rotate the service account key this roleset uses to generate access tokens.

        This does not recreate the roleset service account.

        Supported methods:
            POST: /{mount_point}/roleset/{name}/rotate-key. Produces: 204 (empty body)

        :param name: Name of the role.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/roleset/{name}/rotate-key",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
        )

    def read_roleset(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Read a roleset.

        Supported methods:
            GET: /{mount_point}/roleset/{name}. Produces: 200 application/json

        :param name: Name of the role.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/roleset/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def list_rolesets(self, mount_point=DEFAULT_MOUNT_POINT):
        """List configured rolesets.

        Supported methods:
            LIST: /{mount_point}/rolesets. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/rolesets", mount_point=mount_point
        )
        return self._adapter.list(
            url=api_path,
        )

    def delete_roleset(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete an existing roleset by the given name.

        Supported methods:
            DELETE: /{mount_point}/roleset/{name} Produces: 200 application/json

        :param name: Name of the role.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/roleset/{name}",
            name=name,
            mount_point=mount_point,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def generate_oauth2_access_token(self, roleset, mount_point=DEFAULT_MOUNT_POINT):
        """Generate an OAuth2 token with the scopes defined on the roleset.

        This OAuth access token can be used in GCP API calls, e.g. curl -H "Authorization: Bearer $TOKEN" ...

        Supported methods:
            GET: /{mount_point}/token/{roleset}. Produces: 200 application/json

        :param roleset: Name of an roleset with secret type access_token to generate access_token under.
        :type roleset: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/token/{roleset}",
            mount_point=mount_point,
            roleset=roleset,
        )
        return self._adapter.get(
            url=api_path,
        )

    def generate_service_account_key(
        self,
        roleset,
        key_algorithm="KEY_ALG_RSA_2048",
        key_type="TYPE_GOOGLE_CREDENTIALS_FILE",
        method="POST",
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Generate Secret (IAM Service Account Creds): Service Account Key

        If using GET ('read'), the  optional parameters will be set to their defaults. Use POST if you want to specify
        different values for these params.

        :param roleset: Name of an roleset with secret type service_account_key to generate key under.
        :type roleset: str | unicode
        :param key_algorithm: Key algorithm used to generate key. Defaults to 2k RSA key You probably should not choose
            other values (i.e. 1k),
        :type key_algorithm: str | unicode
        :param key_type: Private key type to generate. Defaults to JSON credentials file.
        :type key_type: str | unicode
        :param method: Supported methods:
            POST: /{mount_point}/key/{roleset}. Produces: 200 application/json
            GET: /{mount_point}/key/{roleset}. Produces: 200 application/json
        :type method: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/key/{roleset}",
            mount_point=mount_point,
            roleset=roleset,
        )

        return self._generate_service_account_key(
            api_path, key_algorithm, key_type, method
        )

    def create_or_update_static_account(
        self,
        name,
        service_account_email,
        bindings=None,
        secret_type=None,
        token_scopes=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create a static account or update an existing static account.

        See static account docs for the GCP secrets backend to learn more about what happens when you create or update a
        static account.

        Supported methods:
            POST: /{mount_point}/static-account/{name}. Produces: 204 (empty body)

        :param name: Name of the static account. Cannot be updated.
        :type name: str | unicode
        :param service_account_email: Email of the GCP service account to manage. Cannot be updated.
        :type service_account_email: str | unicode
        :param bindings: Bindings configuration string (expects HCL or JSON format in raw or base64-encoded string)
        :type bindings: str | unicode
        :param secret_type: Type of secret generated for this static account. Accepted values: access_token,
            service_account_key. Cannot be updated.
        :type secret_type: str | unicode
        :param token_scopes: List of OAuth scopes to assign to access_token secrets generated under this static account
            (access_token static accounts only)
        :type token_scopes: list[str]
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        if secret_type is not None and secret_type not in ALLOWED_SECRETS_TYPES:
            error_msg = 'unsupported secret_type argument provided "{arg}", supported types: "{secret_type}"'
            raise exceptions.ParamValidationError(
                error_msg.format(
                    arg=secret_type,
                    secret_type=",".join(ALLOWED_SECRETS_TYPES),
                )
            )

        if isinstance(bindings, dict):
            bindings = json.dumps(bindings).replace(" ", "")
            logging.debug("bindings: %s" % bindings)

        params = {
            "service_account_email": service_account_email,
        }
        params.update(
            utils.remove_nones(
                {
                    "bindings": bindings,
                    "secret_type": secret_type,
                    "token_scopes": token_scopes,
                }
            )
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/static-account/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def rotate_static_account_key(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Rotate the service account key this static account uses to generate access tokens.

        This does not recreate the service account.

        Supported methods:
            POST: /{mount_point}/static-account/{name}/rotate-key. Produces: 204 (empty body)

        :param name: Name of the static account.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/static-account/{name}/rotate-key",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
        )

    def read_static_account(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Read a static account.

        Supported methods:
            GET: /{mount_point}/static-account/{name}. Produces: 200 application/json

        :param name: Name of the static account.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/static-account/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def list_static_accounts(self, mount_point=DEFAULT_MOUNT_POINT):
        """List configured static accounts.

        Supported methods:
            LIST: /{mount_point}/static-accounts. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/static-accounts", mount_point=mount_point
        )
        return self._adapter.list(
            url=api_path,
        )

    def delete_static_account(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete an existing static account by the given name.

        Supported methods:
            DELETE: /{mount_point}/static-account/{name} Produces: 204 (empty body)

        :param name: Name of the static account.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/static-account/{name}",
            name=name,
            mount_point=mount_point,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def generate_static_account_oauth2_access_token(
        self, name, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Generate an OAuth2 token with the scopes defined on the static account.

        This OAuth access token can be used in GCP API calls, e.g. curl -H "Authorization: Bearer $TOKEN" ...

        Supported methods:
            GET: /{mount_point}/static-account/{name}/token. Produces: 200 application/json

        :param name: Name of a static account with secret type access_token to generate access_token under.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/static-account/{name}/token",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def generate_static_account_service_account_key(
        self,
        name,
        key_algorithm="KEY_ALG_RSA_2048",
        key_type="TYPE_GOOGLE_CREDENTIALS_FILE",
        method="POST",
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Generate Secret (IAM Service Account Creds): Service Account Key

        If using GET ('read'), the  optional parameters will be set to their defaults. Use POST if you want to specify
        different values for these params.

        :param name: Name of a static account with secret type service_account_key to generate key under.
        :type name: str | unicode
        :param key_algorithm: Key algorithm used to generate key. Defaults to 2k RSA key You probably should not choose
            other values (i.e. 1k),
        :type key_algorithm: str | unicode
        :param key_type: Private key type to generate. Defaults to JSON credentials file.
        :type key_type: str | unicode
        :param method: Supported methods:
            POST: /v1/{mount_point}/static-account/{name}/key. Produces: 200 application/json
            GET: /v1/{mount_point}/static-account/{name}/key. Produces: 200 application/json
        :type method: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/static-account/{name}/key",
            mount_point=mount_point,
            name=name,
        )

        return self._generate_service_account_key(
            api_path, key_algorithm, key_type, method
        )

    def create_or_update_impersonated_account(
        self,
        name,
        service_account_email,
        token_scopes=None,
        ttl=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Create an impersonated account or update an existing impersonated account.

        See impersonated account docs for the GCP secrets backend to learn more about what happens when you create or update an
        impersonated account.

        Supported methods:
            POST: /{mount_point}/impersonated-account/{name}. Produces: 204 (empty body)

        :param name: Name of the impersonated account. Cannot be updated.
        :type name: str | unicode
        :param service_account_email: Email of the GCP service account to manage. Cannot be updated.
        :type service_account_email: str | unicode
        :param token_scopes: List of OAuth scopes to assign to access tokens generated under this impersonated account
        :type token_scopes: list[str]
        :param ttl: Lifetime of the token generated. Defaults to 1 hour and is limited to a maximum of 12 hours.
            Uses duration format strings.
        :type ttl: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "service_account_email": service_account_email,
        }
        params.update(
            utils.remove_nones(
                {
                    "token_scopes": token_scopes,
                    "ttl": ttl,
                }
            )
        )
        api_path = utils.format_url(
            "/v1/{mount_point}/impersonated-account/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_impersonated_account(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Read an impersonated account.

        Supported methods:
            GET: /{mount_point}/impersonated-account/{name}. Produces: 200 application/json

        :param name: Name of the impersonated account.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/impersonated-account/{name}",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def list_impersonated_accounts(self, mount_point=DEFAULT_MOUNT_POINT):
        """List configured impersonated accounts.

        Supported methods:
            LIST: /{mount_point}/impersonated-accounts. Produces: 200 application/json

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/impersonated-accounts", mount_point=mount_point
        )
        return self._adapter.list(
            url=api_path,
        )

    def delete_impersonated_account(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """Delete an existing impersonated account by the given name.

        Supported methods:
            DELETE: /{mount_point}/impersonated-account/{name} Produces: 204 (empty body)

        :param name: Name of the impersonated account.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/impersonated-account/{name}",
            name=name,
            mount_point=mount_point,
        )
        return self._adapter.delete(
            url=api_path,
        )

    def generate_impersonated_account_oauth2_access_token(
        self, name, mount_point=DEFAULT_MOUNT_POINT
    ):
        """Generate an OAuth2 token with the scopes defined on the impersonated account.

        This OAuth access token can be used in GCP API calls, e.g. curl -H "Authorization: Bearer $TOKEN" ...

        Supported methods:
            GET: /{mount_point}/impersonated-account/{name}/token. Produces: 200 application/json

        :param name: Name of the impersonated account to generate an access token under.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: dict
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/impersonated-account/{name}/token",
            mount_point=mount_point,
            name=name,
        )
        return self._adapter.get(
            url=api_path,
        )

    def _generate_service_account_key(
        self,
        api_path,
        key_algorithm="KEY_ALG_RSA_2048",
        key_type="TYPE_GOOGLE_CREDENTIALS_FILE",
        method="POST",
    ):
        if method == "POST":
            if key_algorithm not in SERVICE_ACCOUNT_KEY_ALGORITHMS:
                error_msg = 'unsupported key_algorithm argument provided "{arg}", supported algorithms: "{algorithms}"'
                raise exceptions.ParamValidationError(
                    error_msg.format(
                        arg=key_algorithm,
                        algorithms=",".join(SERVICE_ACCOUNT_KEY_ALGORITHMS),
                    )
                )
            if key_type not in SERVICE_ACCOUNT_KEY_TYPES:
                error_msg = 'unsupported key_type argument provided "{arg}", supported types: "{key_types}"'
                raise exceptions.ParamValidationError(
                    error_msg.format(
                        arg=key_type,
                        key_types=",".join(SERVICE_ACCOUNT_KEY_TYPES),
                    )
                )

            params = {
                "key_algorithm": key_algorithm,
                "key_type": key_type,
            }

            response = self._adapter.post(
                url=api_path,
                json=params,
            )
        elif method == "GET":
            response = self._adapter.get(
                url=api_path,
            )
        else:
            error_message = '"method" parameter provided invalid value; POST or GET allowed, "{method}" provided'.format(
                method=method
            )
            raise exceptions.ParamValidationError(error_message)

        return response
