#!/usr/bin/env python
"""RabbitMQ vault secrets backend module."""

from hvac import utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "rabbitmq"


class RabbitMQ(VaultApiBase):
    """RabbitMQ Secrets Engine (API).
    Reference: https://www.vaultproject.io/api/secret/rabbitmq/index.html
    """

    def configure(
        self,
        connection_uri="",
        username="",
        password="",
        verify_connection=True,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """Configure shared information for the rabbitmq secrets engine.

        Supported methods:
            POST: /{mount_point}/config/connection. Produces: 204 (empty body)

        :param connection_uri: Specifies the RabbitMQ connection URI.
        :type connection_uri: str | unicode
        :param username: Specifies the RabbitMQ management administrator username.
        :type username: str | unicode
        :password: Specifies the RabbitMQ management administrator password.
        :type password: str | unicode
        :verify_connection: Specifies whether to verify connection URI, username, and password.
        :type verify_connection: bool
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: rabbitmq).
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "connection_uri": connection_uri,
            "verify_connection": verify_connection,
            "username": username,
            "password": password,
        }

        api_path = utils.format_url(
            "/v1/{mount_point}/config/connection", mount_point=mount_point
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def configure_lease(self, ttl, max_ttl, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint configures the lease settings for generated credentials.

        :param ttl: Specifies the lease ttl provided in seconds.
        :type ttl: int
        :param max_ttl: Specifies the maximum ttl provided in seconds.
        :type max_ttl: int
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: rabbitmq).
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/config/lease", mount_point)
        params = {
            "ttl": ttl,
            "max_ttl": max_ttl,
        }
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def create_role(
        self, name, tags="", vhosts="", vhost_topics="", mount_point=DEFAULT_MOUNT_POINT
    ):
        """This endpoint creates or updates the role definition.

        :param name:  Specifies the name of the role to create.
        :type name: str | unicode
        :param tags:  Specifies a comma-separated RabbitMQ management tags.
        :type tags: str | unicode
        :param vhosts:  pecifies a map of virtual hosts to permissions.
        :type vhosts: str | unicode
        :param vhost_topics: Specifies a map of virtual hosts and exchanges to topic permissions.
        :type vhost_topics: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: rabbitmq).
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/roles/{}", mount_point, name)
        params = {"tags": tags, "vhosts": vhosts, "vhost_topics": vhost_topics}
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def read_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint queries the role definition.

        :param name:  Specifies the name of the role to read.
        :type name: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: rabbitmq).
        :type mount_point: str | unicode
        :return: The JSON response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/roles/{}", mount_point, name)
        return self._adapter.get(
            url=api_path,
        )

    def delete_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint deletes the role definition.
        Even if the role does not exist, this endpoint will still return a successful response.

        :param name: Specifies the name of the role to delete.
        :type name: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: rabbitmq).
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/roles/{}", mount_point, name)
        return self._adapter.delete(
            url=api_path,
        )

    def generate_credentials(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint generates a new set of dynamic credentials based on the named role.

        :param name: Specifies the name of the role to create credentials against.
        :type name: str | unicode
        :param mount_point: Specifies the place where the secrets engine will be accessible (default: rabbitmq).
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url("/v1/{}/creds/{}", mount_point, name)
        return self._adapter.get(
            url=api_path,
        )
