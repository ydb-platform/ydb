#!/usr/bin/env python
"""Consul methods module."""
from hvac import utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "consul"


class Consul(VaultApiBase):
    """Copnsul Secrets Engine (API).

    Reference: https://www.vaultproject.io/api/secret/consul/index.html
    """

    def configure_access(
        self, address, token, scheme=None, mount_point=DEFAULT_MOUNT_POINT
    ):
        """This endpoint configures the access information for Consul.
        This access information is used so that Vault can communicate with Consul and generate Consul tokens.

        :param address: Specifies the address of the Consul instance, provided as "host:port" like "127.0.0.1:8500".
        :type address: str | unicode
        :param token: Specifies the Consul ACL token to use. This must be a management type token.
        :type token: str | unicode
        :param scheme:  Specifies the URL scheme to use.
        :type scheme: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: consul).
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "address": address,
            "token": token,
        }
        params.update(
            utils.remove_nones(
                {
                    "scheme": scheme,
                }
            )
        )

        api_path = utils.format_url("/v1/{}/config/access", mount_point)
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def create_or_update_role(
        self,
        name,
        policy=None,
        policies=None,
        token_type=None,
        local=None,
        ttl=None,
        max_ttl=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """This endpoint creates or updates the Consul role definition.
        If the role does not exist, it will be created.
        If the role already exists, it will receive updated attributes.

        :param name: Specifies the name of an existing role against which to create this Consul credential.
        :type name: str | unicode
        :param token_type:  Specifies the type of token to create when using this role.
        Valid values are "client" or "management".
        :type token_type: str | unicode
        :param policy: Specifies the base64 encoded ACL policy.
        The ACL format can be found in the Consul ACL documentation (https://www.consul.io/docs/internals/acl.html).
        This is required unless the token_type is management.
        :type policy: str | unicode
        :param policies: The list of policies to assign to the generated token.
        This is only available in Consul 1.4 and greater.
        :type policies: list
        :param local: Indicates that the token should not be replicated globally
        and instead be local to the current datacenter. Only available in Consul 1.4 and greater.
        :type local: bool
        :param ttl: Specifies the TTL for this role.
        This is provided as a string duration with a time suffix like "30s" or "1h" or as seconds.
        If not provided, the default Vault TTL is used.
        :type ttl: str | unicode
        :param max_ttl: Specifies the max TTL for this role.
        This is provided as a string duration with a time suffix like "30s" or "1h" or as seconds.
        If not provided, the default Vault Max TTL is used.
        :type max_ttl: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: consul).
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/roles/{}", mount_point, name)

        params = utils.remove_nones(
            {
                "token_type": token_type,
                "policy": policy,
                "policies": policies,
                "local": local,
                "ttl": ttl,
                "max_ttl": max_ttl,
            }
        )

        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint queries for information about a Consul role with the given name.
        If no role exists with that name, a 404 is returned.

        :param name: Specifies the name of the role to query.
        :type name: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: consul).
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url("/v1/{}/roles/{}", mount_point, name)

        return self._adapter.get(
            url=api_path,
        )

    def list_roles(self, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint lists all existing roles in the secrets engine.

        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url("/v1/{}/roles", mount_point)
        return self._adapter.list(
            url=api_path,
        )

    def delete_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint deletes a Consul role with the given name.
        Even if the role does not exist, this endpoint will still return a successful response.

        :param name: Specifies the name of the role to delete.
        :type name: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: consul).
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/roles/{}", mount_point, name)
        return self._adapter.delete(
            url=api_path,
        )

    def generate_credentials(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint generates a dynamic Consul token based on the given role definition.

        :param name: Specifies the name of an existing role against which to create this Consul credential.
        :type name: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: consul).
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/creds/{}", mount_point, name)

        return self._adapter.get(
            url=api_path,
        )
