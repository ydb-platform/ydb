from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Organizations:
    """Auth0 organizations endpoints

    Args:
        domain (str): Your Auth0 domain, e.g: 'username.auth0.com'

        token (str): Management API v2 Token

        telemetry (bool, optional): Enable or disable Telemetry
            (defaults to True)

        timeout (float or tuple, optional): Change the requests
            connect and read timeout. Pass a tuple to specify
            both values separately or a float to set both to it.
            (defaults to 5.0 for both)

        protocol (str, optional): Protocol to use when making requests.
            (defaults to "https")

        rest_options (RestClientOptions): Pass an instance of
            RestClientOptions to configure additional RestClient
            options, such as rate-limit retries.
            (defaults to None)
    """

    def __init__(
        self,
        domain: str,
        token: str,
        telemetry: bool = True,
        timeout: TimeoutType = 5.0,
        protocol: str = "https",
        rest_options: RestClientOptions | None = None,
    ) -> None:
        self.domain = domain
        self.protocol = protocol
        self.client = RestClient(
            jwt=token, telemetry=telemetry, timeout=timeout, options=rest_options
        )

    def _url(self, *args: str | None) -> str:
        url = f"{self.protocol}://{self.domain}/api/v2/organizations"
        for p in args:
            if p is not None:
                url = f"{url}/{p}"
        return url

    # Organizations
    def all_organizations(
        self,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = True,
        from_param: str | None = None,
        take: int | None = None,
    ):
        """Retrieves a list of all the organizations.

        Args:
            page (int): The result's page number (zero based). When not set,
                the default value is up to the server.

            per_page (int, optional): The amount of entries per page. When not set,
                the default value is up to the server.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to True.

            from_param (str, optional): Checkpoint Id from which to begin retrieving results.
                You can limit the number of entries using the take parameter.

            take (int, optional): The total amount of entries to retrieve when
                using the from parameter. When not set, the default value is up to the server.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/get_organizations
        """

        params = {
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
            "from": from_param,
            "take": take,
        }

        return self.client.get(self._url(), params=params)

    def get_organization_by_name(self, name: str | None = None) -> dict[str, Any]:
        """Retrieves an organization given its name.

        Args:
           name (str): The name of the organization to retrieve.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/get_name_by_name
        """
        params = {}

        return self.client.get(self._url("name", name), params=params)

    def get_organization(self, id: str) -> dict[str, Any]:
        """Retrieves an organization by its ID.

        Args:
           id (str): Id of organization to retrieve.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/get_organizations_by_id
        """
        params = {}

        return self.client.get(self._url(id), params=params)

    def create_organization(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a new organization.

        Args:
           body (dict): Attributes for the new organization.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/post_organizations
        """

        return self.client.post(self._url(), data=body)

    def update_organization(self, id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Modifies an organization.

        Args:
           id (str): the ID of the organization.

           body (dict): Attributes to modify.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/patch_organizations_by_id
        """

        return self.client.patch(self._url(id), data=body)

    def delete_organization(self, id: str) -> Any:
        """Deletes an organization and all its related assets.

        Args:
           id (str): Id of organization to delete.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/delete_organizations_by_id
        """

        return self.client.delete(self._url(id))

    # Organization Connections
    def all_organization_connections(
        self, id: str, page: int | None = None, per_page: int | None = None
    ) -> list[dict[str, Any]]:
        """Retrieves a list of all the organization connections.

        Args:
           id (str): the ID of the organization.

           page (int): The result's page number (zero based). When not set,
              the default value is up to the server.

           per_page (int, optional): The amount of entries per page. When not set,
              the default value is up to the server.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/get_enabled_connections
        """
        params = {"page": page, "per_page": per_page}
        return self.client.get(self._url(id, "enabled_connections"), params=params)

    def get_organization_connection(
        self, id: str, connection_id: str
    ) -> dict[str, Any]:
        """Retrieves an organization connection by its ID.

        Args:
           id (str): the ID of the organization.

           connection_id (str): the ID of the connection.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/get_enabled_connections_by_connectionId
        """
        params = {}

        return self.client.get(
            self._url(id, "enabled_connections", connection_id), params=params
        )

    def create_organization_connection(
        self, id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Adds a connection to an organization.

        Args:
           id (str): the ID of the organization.

           body (dict): Attributes for the connection to add.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/post_enabled_connections
        """

        return self.client.post(self._url(id, "enabled_connections"), data=body)

    def update_organization_connection(
        self, id: str, connection_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Modifies an organization.

        Args:
           id (str): the ID of the organization.

           connection_id (str): the ID of the connection to update.

           body (dict): Attributes to modify.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/patch_enabled_connections_by_connectionId
        """

        return self.client.patch(
            self._url(id, "enabled_connections", connection_id), data=body
        )

    def delete_organization_connection(self, id: str, connection_id: str) -> Any:
        """Deletes a connection from the given organization.

        Args:
           id (str): Id of organization.

           connection_id (str): the ID of the connection to delete.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/delete_enabled_connections_by_connectionId
        """

        return self.client.delete(self._url(id, "enabled_connections", connection_id))

    # Organization Members
    def all_organization_members(
        self,
        id: str,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = True,
        from_param: str | None = None,
        take: int | None = None,
        fields: list[str] | None = None,
        include_fields: bool = True,
    ):
        """Retrieves a list of all the organization members.

        Member roles are not sent by default. Use `fields=roles` to retrieve the roles assigned to each listed member.
        To use this parameter, you must include the `read:organization_member_roles scope` in the token.

        Args:
            id (str): the ID of the organization.

            page (int): The result's page number (zero based). When not set,
                the default value is up to the server.

            per_page (int, optional): The amount of entries per page. When not set,
                the default value is up to the server.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to True.

            from_param (str, optional): Checkpoint Id from which to begin retrieving results.
                You can limit the number of entries using the take parameter.

            take (int, optional): The total amount of entries to retrieve when
                using the from parameter. When not set, the default value is up to the server.

            fields (list of str, optional): A list of fields to include or
              exclude from the result (depending on include_fields). If fields is left blank,
              all fields (except roles) are returned.

            include_fields (bool, optional): True if the fields specified are
              to be included in the result, False otherwise. Defaults to True.

        See: https://auth0.com/docs/api/management/v2/organizations/get-members
        """

        params = {
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
            "from": from_param,
            "take": take,
            "fields": fields and ",".join(fields) or None,
            "include_fields": str(include_fields).lower(),
        }

        return self.client.get(self._url(id, "members"), params=params)

    def create_organization_members(
        self, id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Adds members to an organization.

        Args:
           id (str): the ID of the organization.

           body (dict): Attributes from the members to add.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/post_members
        """

        return self.client.post(self._url(id, "members"), data=body)

    def delete_organization_members(self, id: str, body: dict[str, Any]) -> Any:
        """Deletes members from the given organization.

        Args:
           id (str): Id of organization.

           body (dict): Attributes from the members to delete

        See: https://auth0.com/docs/api/management/v2#!/Organizations/delete_members
        """

        return self.client.delete(self._url(id, "members"), data=body)

    # Organization Member Roles
    def all_organization_member_roles(
        self,
        id: str,
        user_id: str,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = False,
    ) -> list[dict[str, Any]]:
        """Retrieves a list of all the roles from the given organization member.

        Args:
           id (str): the ID of the organization.

           user_id (str): the ID of the user member of the organization.

           page (int): The result's page number (zero based). When not set,
              the default value is up to the server.

           per_page (int, optional): The amount of entries per page. When not set,
              the default value is up to the server.

           include_totals (bool, optional): True if the query summary is
              to be included in the result, False otherwise. Defaults to False.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/get_organization_member_roles
        """
        params = {
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower()
        }
        return self.client.get(
            self._url(id, "members", user_id, "roles"), params=params
        )

    def create_organization_member_roles(
        self, id: str, user_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Adds roles to a member of an organization.

        Args:
           id (str): the ID of the organization.

           user_id (str): the ID of the user member of the organization.

           body (dict): Attributes from the members to add.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/post_organization_member_roles
        """

        return self.client.post(self._url(id, "members", user_id, "roles"), data=body)

    def delete_organization_member_roles(
        self, id: str, user_id: str, body: dict[str, Any]
    ) -> Any:
        """Deletes roles from a member of an organization.

        Args:
           id (str): Id of organization.

           user_id (str): the ID of the user member of the organization.

           body (dict): Attributes from the members to delete

        See: https://auth0.com/docs/api/management/v2#!/Organizations/delete_organization_member_roles
        """

        return self.client.delete(self._url(id, "members", user_id, "roles"), data=body)

    # Organization Invitations
    def all_organization_invitations(
        self,
        id: str,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = False,
    ):
        """Retrieves a list of all the organization invitations.

        Args:
           id (str): the ID of the organization.

           page (int): The result's page number (zero based). When not set,
              the default value is up to the server.

           per_page (int, optional): The amount of entries per page. When not set,
              the default value is up to the server.

           include_totals (bool, optional): True if the query summary is
              to be included in the result, False otherwise. Defaults to False.
              NOTE: returns start and limit, total count is not yet supported

        See: https://auth0.com/docs/api/management/v2#!/Organizations/get_invitations
        """
        params = {
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
        }

        return self.client.get(self._url(id, "invitations"), params=params)

    def get_organization_invitation(self, id: str, invitaton_id: str) -> dict[str, Any]:
        """Retrieves an organization invitation by its ID.

        Args:
           id (str): the ID of the organization.

           invitaton_id (str): the ID of the invitation.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/get_invitations_by_invitation_id
        """
        params = {}

        return self.client.get(
            self._url(id, "invitations", invitaton_id), params=params
        )

    def create_organization_invitation(
        self, id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Create an invitation to an organization.

        Args:
           id (str): the ID of the organization.

           body (dict): Attributes for the invitation to create.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/post_invitations
        """

        return self.client.post(self._url(id, "invitations"), data=body)

    def delete_organization_invitation(self, id: str, invitation_id: str) -> Any:
        """Deletes an invitation from the given organization.

        Args:
           id (str): Id of organization.

           invitation_id (str): the ID of the invitation to delete.

        See: https://auth0.com/docs/api/management/v2#!/Organizations/delete_invitations_by_invitation_id
        """

        return self.client.delete(self._url(id, "invitations", invitation_id))

    def get_client_grants(
        self,
        id: str,
        audience: str | None = None,
        client_id: str | None = None,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = False,
    ):
        """Get client grants associated to an organization.

        Args:
            id (str): Id of organization.

            audience (str, optional): URL encoded audience of a Resource Server
                to filter.

            client_id (string, optional): The id of a client to filter.

            page (int, optional): The result's page number (zero based). When not set,
                the default value is up to the server.

            per_page (int, optional): The amount of entries per page. When not set,
                the default value is up to the server.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to False.
        """
        params = {
            "audience": audience,
            "client_id": client_id,
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
        }

        return self.client.get(self._url(id, "client-grants"), params=params)

    def add_client_grant(self, id: str, grant_id: str) -> dict[str, Any]:
        """Associate a client grant with an organization.

        Args:
           id (str): the ID of the organization.

           grant_id (string) A Client Grant ID to add to the organization.
        """

        return self.client.post(
            self._url(id, "client-grants"), data={"grant_id": grant_id}
        )

    def delete_client_grant(self, id: str, grant_id: str) -> dict[str, Any]:
        """Remove a client grant from an organization.

        Args:
           id (str): the ID of the organization.

           grant_id (string) A Client Grant ID to remove from the organization.
        """

        return self.client.delete(self._url(id, "client-grants", grant_id))
