from __future__ import annotations

from typing import Any

from .base import AuthenticationBase


class Database(AuthenticationBase):
    """Database & Active Directory / LDAP Authentication.

    Args:
        domain (str): Your auth0 domain (e.g: username.auth0.com)
    """

    def signup(
        self,
        email: str,
        password: str,
        connection: str,
        username: str | None = None,
        user_metadata: dict[str, Any] | None = None,
        given_name: str | None = None,
        family_name: str | None = None,
        name: str | None = None,
        nickname: str | None = None,
        picture: str | None = None,
    ) -> dict[str, Any]:
        """Signup using email and password.

        Args:
           email (str): The user's email address.

           password (str): The user's desired password.

           connection (str): The name of the database connection where this user should be created.

           username (str, optional): The user's username, if required by the database connection.

           user_metadata (dict, optional): Additional key-value information to store for the user.
                    Some limitations apply, see: https://auth0.com/docs/metadata#metadata-restrictions

           given_name (str, optional): The user's given name(s).

           family_name (str, optional): The user's family name(s).

           name (str, optional): The user's full name.

           nickname (str, optional): The user's nickname.

           picture (str, optional): A URI pointing to the user's picture.


        See: https://auth0.com/docs/api/authentication#signup
        """
        body: dict[str, Any] = {
            "client_id": self.client_id,
            "email": email,
            "password": password,
            "connection": connection,
        }
        if username:
            body.update({"username": username})
        if user_metadata:
            body.update({"user_metadata": user_metadata})
        if given_name:
            body.update({"given_name": given_name})
        if family_name:
            body.update({"family_name": family_name})
        if name:
            body.update({"name": name})
        if nickname:
            body.update({"nickname": nickname})
        if picture:
            body.update({"picture": picture})

        data: dict[str, Any] = self.post(
            f"{self.protocol}://{self.domain}/dbconnections/signup", data=body
        )
        return data

    def change_password(
        self,
        email: str,
        connection: str,
        password: str | None = None,
        organization: str | None = None,
    ) -> str:
        """Asks to change a password for a given user.

        email (str): The user's email address.

        connection (str): The name of the database connection where this user should be created.

        organization (str, optional): The id of the Organization associated with the user.
        """
        body = {
            "client_id": self.client_id,
            "email": email,
            "connection": connection,
        }
        if organization:
            body["organization"] = organization

        data: str = self.post(
            f"{self.protocol}://{self.domain}/dbconnections/change_password",
            data=body,
        )
        return data
