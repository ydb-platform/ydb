#!/usr/bin/env python
"""Database methods module."""
from hvac import utils
from hvac.api.vault_api_base import VaultApiBase

DEFAULT_MOUNT_POINT = "database"


class Database(VaultApiBase):
    """Database Secrets Engine (API).

    Reference: https://www.vaultproject.io/api/secret/databases/index.html
    """

    def configure(
        self,
        name,
        plugin_name,
        verify_connection=None,
        allowed_roles=None,
        root_rotation_statements=None,
        mount_point=DEFAULT_MOUNT_POINT,
        *args,
        **kwargs
    ):
        """This endpoint configures the connection string used to communicate with the desired database.
        In addition to the parameters listed here, each Database plugin has additional,
        database plugin specific, parameters for this endpoint.
        Please read the HTTP API for the plugin you'd wish to configure to see the full list of additional parameters.

        :param name: Specifies the name for this database connection. This is specified as part of the URL.
        :type name: str | unicode
        :param plugin_name: Specifies the name of the plugin to use for this connection.
        :type plugin_name: str | unicode
        :param verify_connection: Specifies if the connection is verified during initial configuration.
        :type verify_connection: bool
        :param allowed_roles: List of the roles allowed to use this connection. Defaults to empty (no roles),
            if contains a "*" any role can use this connection.
        :type allowed_roles: list
        :param root_rotation_statements: Specifies the database statements to be executed to rotate
            the root user's credentials.
        :type root_rotation_statements: list
        :return: The response of the request.
        :rtype: requests.Response
        """
        params = {
            "plugin_name": plugin_name,
        }
        params.update(
            utils.remove_nones(
                {
                    "allowed_roles": allowed_roles,
                    "verify_connection": verify_connection,
                    "root_rotation_statements": root_rotation_statements,
                }
            )
        )

        params.update(kwargs)

        api_path = utils.format_url(
            "/v1/{mount_point}/config/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.post(
            url=api_path,
            json=params,
        )

    def rotate_root_credentials(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint is used to rotate the root superuser credentials stored for the database connection.
        This user must have permissions to update its own password.

        :param name: Specifies the name of the connection to rotate.
        :type name: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/rotate-root/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.post(
            url=api_path,
        )

    def read_connection(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint returns the configuration settings for a connection.

        :param name: Specifies the name of the connection to read.
        :type name: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url(
            "/v1/{mount_point}/config/{name}", mount_point=mount_point, name=name
        )

        return self._adapter.get(
            url=api_path,
        )

    def list_connections(self, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint returns a list of available connections.

        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url("/v1/{mount_point}/config", mount_point=mount_point)
        return self._adapter.list(
            url=api_path,
        )

    def delete_connection(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint deletes a connection.


        :param name: Specifies the name of the connection to delete.
        :type name: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/config/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.delete(
            url=api_path,
        )

    def reset_connection(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint closes a connection and it's underlying plugin and
        restarts it with the configuration stored in the barrier.

        :param name: Specifies the name of the connection to reset.
        :type name: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/reset/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.post(
            url=api_path,
        )

    def create_role(
        self,
        name,
        db_name,
        creation_statements,
        default_ttl=None,
        max_ttl=None,
        revocation_statements=None,
        rollback_statements=None,
        renew_statements=None,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """This endpoint creates or updates a role definition.

        :param name: Specifies the database role to manage.
        :type name: str | unicode
        :param db_name: The name of the database connection to use for this role.
        :type db_name: str | unicode
        :param creation_statements: Specifies the database statements executed to create and configure a user.
        :type creation_statements: list
        :param default_ttl: Specifies the TTL for the leases associated with this role.
        :type default_ttl: int
        :param max_ttl: Specifies the maximum TTL for the leases associated with this role.
        :type max_ttl: int
        :param revocation_statements: Specifies the database statements to be executed to revoke a user.
        :type revocation_statements: list
        :param rollback_statements: Specifies the database statements to be executed to rollback
            a create operation in the event of an error.
        :type rollback_statements: list
        :param renew_statements: Specifies the database statements to be executed to renew a user.
        :type renew_statements: list
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        params = {
            "db_name": db_name,
            "creation_statements": creation_statements,
        }
        params.update(
            utils.remove_nones(
                {
                    "default_ttl": default_ttl,
                    "max_ttl": max_ttl,
                    "revocation_statements": revocation_statements,
                    "rollback_statements": rollback_statements,
                    "renew_statements": renew_statements,
                }
            )
        )

        api_path = utils.format_url(
            "/v1/{mount_point}/roles/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.post(url=api_path, json=params)

    def create_static_role(
        self,
        name,
        db_name,
        username,
        rotation_statements,
        rotation_period=86400,
        mount_point=DEFAULT_MOUNT_POINT,
    ):
        """This endpoint creates or updates a static role definition.

        :param name: Specifies the name of the role to create.
        :type name: str | unicode
        :param db_name: The name of the database connection to use for this role.
        :type db_name: str | unicode
        :param username: Specifies the database username that the Vault role `name` above corresponds to.
        :type username: str | unicode
        :param rotation_statements: Specifies the database statements to be executed to rotate the password for the configured database user.
            Not every plugin type will support this functionality. See the plugin's API page for more information on support and
            formatting for this parameter.
        :type rotation_statements: list
        :param rotation_period: Specifies the amount of time Vault should wait before rotating the password. The minimum is 5 seconds.
        :type rotation_period: int
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        params = {
            "db_name": db_name,
            "username": username,
            "rotation_statements": rotation_statements,
            "rotation_period": rotation_period,
        }

        api_path = utils.format_url(
            "/v1/{mount_point}/static-roles/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.post(url=api_path, json=params)

    def read_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint queries the role definition.

        :param name: Specifies the name of the role to read.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url(
            "/v1/{mount_point}/roles/{name}", mount_point=mount_point, name=name
        )

        return self._adapter.get(
            url=api_path,
        )

    def read_static_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint queries the static role definition.

        :param name: Specifies the name of the role to read.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url(
            "/v1/{mount_point}/static-roles/{name}", mount_point=mount_point, name=name
        )

        return self._adapter.get(
            url=api_path,
        )

    def list_roles(self, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint returns a list of available roles.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url("/v1/{mount_point}/roles", mount_point=mount_point)
        return self._adapter.list(
            url=api_path,
        )

    def list_static_roles(self, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint returns a list of available static roles.

        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url(
            "/v1/{mount_point}/static-roles", mount_point=mount_point
        )
        return self._adapter.list(
            url=api_path,
        )

    def delete_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint deletes the role definition.

        :param name: Specifies the name of the role to delete.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/roles/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.delete(
            url=api_path,
        )

    def delete_static_role(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint deletes the static role definition.

        :param name: Specifies the name of the role to delete.
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """
        api_path = utils.format_url(
            "/v1/{mount_point}/static-roles/{name}", mount_point=mount_point, name=name
        )
        return self._adapter.delete(
            url=api_path,
        )

    def generate_credentials(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint generates a new set of dynamic credentials based on the named role.

        :param name: Specifies the name of the role to create credentials against
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url(
            "/v1/{mount_point}/creds/{name}", mount_point=mount_point, name=name
        )

        return self._adapter.get(
            url=api_path,
        )

    def get_static_credentials(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint returns the current credentials based on the named static role.

        :param name: Specifies the name of the role to create credentials against
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url(
            "/v1/{mount_point}/static-creds/{name}", mount_point=mount_point, name=name
        )

        return self._adapter.get(
            url=api_path,
        )

    def rotate_static_role_credentials(self, name, mount_point=DEFAULT_MOUNT_POINT):
        """This endpoint is used to rotate the Static Role credentials stored for a given role name.
        While Static Roles are rotated automatically by Vault at configured rotation periods,
        users can use this endpoint to manually trigger a rotation to change the stored password and
        reset the TTL of the Static Role's password.

        :param name: Specifies the name of the role to create credentials against
        :type name: str | unicode
        :param mount_point: The "path" the method/backend was mounted on.
        :type mount_point: str | unicode
        :return: The response of the request.
        :rtype: requests.Response
        """

        api_path = utils.format_url(
            "/v1/{mount_point}/rotate-role/{name}", mount_point=mount_point, name=name
        )

        return self._adapter.post(
            url=api_path,
        )
