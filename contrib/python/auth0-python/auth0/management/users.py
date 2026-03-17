from __future__ import annotations

from typing import Any, List  # List is being used as list is already a method.

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Users:
    """Auth0 users endpoints

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

    def _url(self, id: str | None = None) -> str:
        url = f"{self.protocol}://{self.domain}/api/v2/users"
        if id is not None:
            return f"{url}/{id}"
        return url

    def list(
        self,
        page: int = 0,
        per_page: int = 25,
        sort: str | None = None,
        connection: str | None = None,
        q: str | None = None,
        search_engine: str | None = None,
        include_totals: bool = True,
        fields: List[str] | None = None,
        include_fields: bool = True,
    ):
        """List or search users.

        Args:
            page (int, optional): The result's page number (zero based). By default,
               retrieves the first page of results.

            per_page (int, optional): The amount of entries per page. By default,
               retrieves 25 results per page.

            sort (str, optional): The field to use for sorting.
                1 == ascending and -1 == descending. (e.g: email:1)
                When not set, the default value is up to the server.

            connection (str, optional): Connection filter.

            q (str, optional): Query in Lucene query string syntax. Only fields
                in app_metadata, user_metadata or the normalized user profile
                are searchable.

            search_engine (str, optional): The version of the search_engine to use
                when querying for users. Will default to the latest version available.
                See: https://auth0.com/docs/users/search.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to True.

            fields (list of str, optional): A list of fields to include or
                exclude from the result (depending on include_fields). Leave empty to
                retrieve all fields.

            include_fields (bool, optional): True if the fields specified are
                to be include in the result, False otherwise. Defaults to True.

        See: https://auth0.com/docs/api/management/v2#!/Users/get_users
        """
        params = {
            "per_page": per_page,
            "page": page,
            "include_totals": str(include_totals).lower(),
            "sort": sort,
            "connection": connection,
            "fields": fields and ",".join(fields) or None,
            "include_fields": str(include_fields).lower(),
            "q": q,
            "search_engine": search_engine,
        }
        return self.client.get(self._url(), params=params)

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Creates a new user.

        Args:
            body (dict): the attributes to set on the user to create.

        See: https://auth0.com/docs/api/v2#!/Users/post_users
        """
        return self.client.post(self._url(), data=body)

    def get(
        self, id: str, fields: List[str] | None = None, include_fields: bool = True
    ) -> dict[str, Any]:
        """Get a user.

        Args:
            id (str): The user_id of the user to retrieve.

            fields (list of str, optional): A list of fields to include or
                exclude from the result (depending on include_fields). Leave empty to
                retrieve all fields.

            include_fields (bool, optional): True if the fields specified are
                to be included in the result, False otherwise. Defaults to True.

        See: https://auth0.com/docs/api/management/v2#!/Users/get_users_by_id
        """
        params = {
            "fields": fields and ",".join(fields) or None,
            "include_fields": str(include_fields).lower(),
        }

        return self.client.get(self._url(id), params=params)

    def delete(self, id: str) -> Any:
        """Delete a user.

        Args:
            id (str): The user_id of the user to delete.

        See: https://auth0.com/docs/api/management/v2#!/Users/delete_users_by_id
        """
        return self.client.delete(self._url(id))

    def update(self, id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Update a user with the attributes passed in 'body'

        Args:
            id (str): The user_id of the user to update.

            body (dict): The attributes of the user to update.

        See: https://auth0.com/docs/api/v2#!/Users/patch_users_by_id
        """
        return self.client.patch(self._url(id), data=body)

    def list_organizations(
        self, id: str, page: int = 0, per_page: int = 25, include_totals: bool = True
    ):
        """List the organizations that the user is member of.

        Args:
            id (str): The user's id.

            page (int, optional): The result's page number (zero based). By default,
               retrieves the first page of results.

            per_page (int, optional): The amount of entries per page. By default,
               retrieves 25 results per page.

            include_totals (bool, optional): True if the query summary is
               to be included in the result, False otherwise. Defaults to True.

        See https://auth0.com/docs/api/management/v2#!/Users/get_organizations
        """
        params = {
            "per_page": per_page,
            "page": page,
            "include_totals": str(include_totals).lower(),
        }

        url = self._url(f"{id}/organizations")
        return self.client.get(url, params=params)

    def list_roles(
        self, id: str, page: int = 0, per_page: int = 25, include_totals: bool = True
    ):
        """List the roles associated with a user.

        Args:
            id (str): The user's id.

            page (int, optional): The result's page number (zero based). By default,
               retrieves the first page of results.

            per_page (int, optional): The amount of entries per page. By default,
               retrieves 25 results per page.

            include_totals (bool, optional): True if the query summary is
               to be included in the result, False otherwise. Defaults to True.

        See https://auth0.com/docs/api/management/v2#!/Users/get_user_roles
        """
        params = {
            "per_page": per_page,
            "page": page,
            "include_totals": str(include_totals).lower(),
        }

        url = self._url(f"{id}/roles")
        return self.client.get(url, params=params)

    def remove_roles(self, id: str, roles: List[str]) -> Any:
        """Removes an array of roles from a user.

        Args:
            id (str): The user's id.

            roles (list of str): A list of roles ids to unassociate from the user.

        See https://auth0.com/docs/api/management/v2#!/Users/delete_user_roles
        """
        url = self._url(f"{id}/roles")
        body = {"roles": roles}
        return self.client.delete(url, data=body)

    def add_roles(self, id: str, roles: List[str]) -> dict[str, Any]:
        """Associate an array of roles with a user.

        Args:
            id (str): The user's id.

            roles (list of str): A list of roles ids to associated with the user.

        See https://auth0.com/docs/api/management/v2#!/Users/post_user_roles
        """
        url = self._url(f"{id}/roles")
        body = {"roles": roles}
        return self.client.post(url, data=body)

    def list_permissions(
        self, id: str, page: int = 0, per_page: int = 25, include_totals: bool = True
    ):
        """List the permissions associated to the user.

        Args:
            id (str): The user's id.

            page (int, optional): The result's page number (zero based). By default,
               retrieves the first page of results.

            per_page (int, optional): The amount of entries per page. By default,
               retrieves 25 results per page.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to True.

        See https://auth0.com/docs/api/management/v2#!/Users/get_permissions
        """

        params = {
            "per_page": per_page,
            "page": page,
            "include_totals": str(include_totals).lower(),
        }
        url = self._url(f"{id}/permissions")
        return self.client.get(url, params=params)

    def remove_permissions(self, id: str, permissions: List[str]) -> Any:
        """Removes permissions from a user.

        Args:
            id (str): The user's id.

            permissions (list of str): A list of permission ids to unassociate from the user.

        See https://auth0.com/docs/api/management/v2#!/Users/delete_permissions
        """
        url = self._url(f"{id}/permissions")
        body = {"permissions": permissions}
        return self.client.delete(url, data=body)

    def add_permissions(self, id: str, permissions: List[str]) -> dict[str, Any]:
        """Assign permissions to a user.

        Args:
            id (str): The user's id.

            permissions (list of str): A list of permission ids to associated with the user.

        See https://auth0.com/docs/api/management/v2#!/Users/post_permissions
        """
        url = self._url(f"{id}/permissions")
        body = {"permissions": permissions}
        return self.client.post(url, data=body)

    def delete_multifactor(self, id: str, provider: str) -> Any:
        """Delete a user's multifactor provider.

        Args:
            id (str): The user's id.

            provider (str): The multifactor provider. Supported values 'duo'
                or 'google-authenticator'.

        See: https://auth0.com/docs/api/management/v2#!/Users/delete_multifactor_by_provider
        """
        url = self._url(f"{id}/multifactor/{provider}")
        return self.client.delete(url)

    def delete_authenticators(self, id: str) -> Any:
        """Delete a user's MFA enrollments.

        Args:
            id (str): The user's id.

        See: https://auth0.com/docs/api/management/v2#!/Users/delete_authenticators
        """
        url = self._url(f"{id}/authenticators")
        return self.client.delete(url)

    def unlink_user_account(self, id: str, provider: str, user_id: str) -> Any:
        """Unlink a user account

        Args:
            id (str): The user_id of the user identity.

            provider (str): The type of identity provider (e.g: facebook).

            user_id (str): The unique identifier for the user for the identity.

        See: https://auth0.com/docs/api/management/v2#!/Users/delete_user_identity_by_user_id
        """
        url = self._url(f"{id}/identities/{provider}/{user_id}")
        return self.client.delete(url)

    def link_user_account(self, user_id: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        """Link user accounts.

        Links the account specified in the body (secondary account) to the
        account specified by the id param of the URL (primary account).

        Args:
            id (str): The user_id of the primary identity where you are linking
                the secondary account to.

            body (dict): the attributes to send as part of this request.

        See: https://auth0.com/docs/api/v2#!/Users/post_identities
        """
        url = self._url(f"{user_id}/identities")
        return self.client.post(url, data=body)

    def regenerate_recovery_code(self, user_id: str) -> dict[str, Any]:
        """Removes the current recovery token, generates and returns a new one

        Args:
            user_id (str):  The user_id of the user identity.

        See: https://auth0.com/docs/api/management/v2#!/Users/post_recovery_code_regeneration
        """
        url = self._url(f"{user_id}/recovery-code-regeneration")
        return self.client.post(url)

    def get_guardian_enrollments(self, user_id: str) -> dict[str, Any]:
        """Retrieve the first confirmed Guardian enrollment for a user.

        Args:
            user_id (str):  The user_id of the user to retrieve.

        See: https://auth0.com/docs/api/management/v2#!/Users/get_enrollments
        """
        url = self._url(f"{user_id}/enrollments")
        return self.client.get(url)

    def get_log_events(
        self,
        user_id: str,
        page: int = 0,
        per_page: int = 50,
        sort: str | None = None,
        include_totals: bool = False,
    ):
        """Retrieve every log event for a specific user id.

        Args:
            user_id (str):  The user_id of the logs to retrieve.

            page (int, optional): The result's page number (zero based). By default,
                retrieves the first page of results.

            per_page (int, optional): The amount of entries per page. By default,
                retrieves 50 results per page.

            sort (str, optional):  The field to use for sorting. Use field:order
                where order is 1 for ascending and -1 for descending.
                For example date:-1
                When not set, the default value is up to the server.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to False.

        See: https://auth0.com/docs/api/management/v2#!/Users/get_logs_by_user
        """

        params = {
            "per_page": per_page,
            "page": page,
            "include_totals": str(include_totals).lower(),
            "sort": sort,
        }

        url = self._url(f"{user_id}/logs")
        return self.client.get(url, params=params)

    def invalidate_remembered_browsers(self, user_id: str) -> dict[str, Any]:
        """Invalidate all remembered browsers across all authentication factors for a user.

        Args:
            user_id (str):  The user_id to invalidate remembered browsers for.

        See: https://auth0.com/docs/api/management/v2#!/Users/post_invalidate_remember_browser
        """

        url = self._url(f"{user_id}/multifactor/actions/invalidate-remember-browser")
        return self.client.post(url)

    def get_authentication_methods(self, user_id: str) -> dict[str, Any]:
        """Gets a list of authentication methods

        Args:
            user_id (str):  The user_id to get a list of authentication methods for.

        See: https://auth0.com/docs/api/management/v2#!/Users/get_authentication_methods
        """

        url = self._url(f"{user_id}/authentication-methods")
        return self.client.get(url)

    def get_authentication_method_by_id(
        self, user_id: str, authentication_method_id: str
    ) -> dict[str, Any]:
        """Gets an authentication method by ID.

        Args:
            user_id (str):  The user_id to get an authentication method by ID for.
            authentication_method_id (str):  The authentication_method_id to get an authentication method by ID for.

        See: https://auth0.com/docs/api/management/v2#!/Users/get_authentication_methods_by_authentication_method_id
        """

        url = self._url(f"{user_id}/authentication-methods/{authentication_method_id}")
        return self.client.get(url)

    def create_authentication_method(
        self, user_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Creates an authentication method for a given user.

        Args:
            user_id (str):  The user_id to create an authentication method for a given user.
            body (dict): the request body to create an authentication method for a given user.

        See: https://auth0.com/docs/api/management/v2#!/Users/post_authentication_methods
        """

        url = self._url(f"{user_id}/authentication-methods")
        return self.client.post(url, data=body)

    def update_authentication_methods(
        self, user_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Updates all authentication methods for a user by replacing them with the given ones.

        Args:
            user_id (str):  The user_id to update all authentication methods for.
            body (dict): the request body to update all authentication methods with.

        See: https://auth0.com/docs/api/management/v2#!/Users/put_authentication_methods
        """

        url = self._url(f"{user_id}/authentication-methods")
        return self.client.put(url, data=body)

    def update_authentication_method_by_id(
        self, user_id: str, authentication_method_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Updates an authentication method.

        Args:
            user_id (str):  The user_id to update an authentication method.
            authentication_method_id (str):  The authentication_method_id to update an authentication method for.
            body (dict): the request body to update an authentication method.

        See: https://auth0.com/docs/api/management/v2#!/Users/patch_authentication_methods_by_authentication_method_id
        """

        url = self._url(f"{user_id}/authentication-methods/{authentication_method_id}")
        return self.client.patch(url, data=body)

    def delete_authentication_methods(self, user_id: str) -> Any:
        """Deletes all authentication methods for the given user.

        Args:
            user_id (str):  The user_id to delete all authentication methods for the given user for.

        See: https://auth0.com/docs/api/management/v2#!/Users/delete_authentication_methods
        """

        url = self._url(f"{user_id}/authentication-methods")
        return self.client.delete(url)

    def delete_authentication_method_by_id(
        self, user_id: str, authentication_method_id: str
    ) -> Any:
        """Deletes an authentication method by ID.

        Args:
            user_id (str):  The user_id to delete an authentication method by ID for.
            authentication_method_id (str):  The authentication_method_id to delete an authentication method by ID for.

        See: https://auth0.com/docs/api/management/v2#!/Users/delete_authentication_methods_by_authentication_method_id
        """

        url = self._url(f"{user_id}/authentication-methods/{authentication_method_id}")
        return self.client.delete(url)

    def list_tokensets(
         self, id: str, page: int = 0, per_page: int = 25, include_totals: bool = True
     ):
         """List all the tokenset(s) associated to the user.
 
         Args:
             id (str): The user's id.
 
             page (int, optional): The result's page number (zero based). By default,
                retrieves the first page of results.
 
             per_page (int, optional): The amount of entries per page. By default,
                retrieves 25 results per page.
 
             include_totals (bool, optional): True if the query summary is
                 to be included in the result, False otherwise. Defaults to True.
 
         See https://auth0.com/docs/api/management/v2#!/Users/get_tokensets
         """
 
         params = {
             "per_page": per_page,
             "page": page,
             "include_totals": str(include_totals).lower(),
         }
         url = self._url(f"{id}/federated-connections-tokensets")
         return self.client.get(url, params=params)
 
    def delete_tokenset_by_id(
         self, user_id: str, tokenset_id: str
     ) -> Any:
         """Deletes an tokenset by ID.
 
         Args:
             user_id (str):  The user_id to delete an authentication method by ID for.
             tokenset_id (str):  The tokenset_id to delete an tokenset by ID for.
 
         See: https://auth0.com/docs/api/management/v2#!/Users/delete_tokenset_by_id
         """
 
         url = self._url(f"{user_id}/federated-connections-tokensets/{tokenset_id}")
         return self.client.delete(url)