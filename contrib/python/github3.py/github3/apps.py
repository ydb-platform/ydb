"""Module containing helpers for dealing with GitHub Apps.

https://developer.github.com/apps/building-github-apps/
"""
import time

import jwt

from . import models
from . import users

TEN_MINUTES_AS_SECONDS = 10 * 60
DEFAULT_JWT_TOKEN_EXPIRATION = TEN_MINUTES_AS_SECONDS
APP_PREVIEW_HEADERS = {
    "Accept": "application/vnd.github.machine-man-preview+json"
}


class App(models.GitHubCore):
    """An object representing a GitHub App.

    .. versionadded:: 1.2.0

    .. seealso::

        `GitHub Apps`_
            Documentation for Apps on GitHub

        `GitHub Apps API Documentation`_
            API documentation of what's available about an App.

    This object has the following attributes:

    .. attribute:: created_at

        A :class:`~datetime.datetime` object representing the day and time
        the App was created.

    .. attribute:: description

        The description of the App provided by the owner.

    .. attribute:: events

        An array of the event types an App receives

    .. attribute:: external_url

        The URL provided for the App by the owner.

    .. attribute:: html_url

        The HTML URL provided for the App by the owner.

    .. attribute:: id

        The unique identifier for the App. This is useful in cases where you
        may want to authenticate either as an App or as a specific
        installation of an App.

    .. attribute:: name

        The display name of the App that the user sees.

    .. attribute:: node_id

        A base64-encoded blob returned by the GitHub API for who knows what
        reason.

    .. attribute:: owner

        A :class:`~github3.users.ShortUser` object representing the GitHub
        user who owns the App.

    .. attribute:: permissions

        A dictionary describing the permissions the App has

    .. attribute:: slug

        A short string used to identify the App

    .. attribute:: updated_at

        A :class:`~datetime.datetime` object representing the day and time
        the App was last updated.

    .. _GitHub Apps:
        https://developer.github.com/apps/
    .. _GitHub Apps API Documentation:
        https://developer.github.com/v3/apps/
    """

    CUSTOM_HEADERS = {
        "Accept": "application/vnd.github.machine-man-preview+json"
    }

    def _update_attributes(self, json):
        self.created_at = self._strptime(json["created_at"])
        self.description = json["description"]
        self.external_url = json["external_url"]
        self.events = json["events"]
        self.html_url = json["html_url"]
        self.id = json["id"]
        self.name = json["name"]
        self.node_id = json["node_id"]
        self.owner = users.ShortUser(json["owner"], self)
        self.permissions = json["permissions"]
        self.slug = json["slug"]
        self.updated_at = self._strptime(json["updated_at"])
        self._api = self.url = self._build_url("apps", self.slug)

    def _repr(self):
        return f'<App ["{self.name}" by {str(self.owner)}]>'


class Installation(models.GitHubCore):
    """An installation of a GitHub App either on a User or Org.

    .. versionadded:: 1.2.0

    This has the following attributes:

    .. attribute:: access_tokens_url
    .. attribute:: account
    .. attribute:: app_id
    .. attribute:: created_at
    .. attribute:: events
    .. attribute:: html_url
    .. attribute:: id
    .. attribute:: permissions
    .. attribute:: repositories_url
    .. attribute:: repository_selection
    .. attribute:: single_file_name
    .. attribute:: target_id
    .. attribute:: target_type
    .. attribute:: updated_at
    """

    def _update_attributes(self, json):
        self.access_tokens_url = json["access_tokens_url"]
        self.account = json["account"]
        self.app_id = json["app_id"]
        self.created_at = self._strptime(json["created_at"])
        self.events = json["events"]
        self.html_url = json["html_url"]
        self.id = json["id"]
        self.permissions = json["permissions"]
        self.repositories_url = json["repositories_url"]
        self.repository_selection = json["repository_selection"]
        self.single_file_name = json["single_file_name"]
        self.target_id = json["target_id"]
        self.target_type = json["target_type"]
        self.updated_at = self._strptime(json["updated_at"])


def create_token(private_key_pem, app_id, expire_in=TEN_MINUTES_AS_SECONDS):
    """Create an encrypted token for the specified App.

    :param bytes private_key_pem:
        The bytes of the private key for this GitHub Application.
    :param int app_id:
        The integer identifier for this GitHub Application.
    :param int expire_in:
        The length in seconds for this token to be valid for.
        Default: 600 seconds (10 minutes)
    :returns:
        Serialized encrypted token.
    :rtype:
        text
    """
    if not isinstance(private_key_pem, bytes):
        raise ValueError('"private_key_pem" parameter must be byte-string')
    now = int(time.time())
    token = jwt.encode(
        payload={"iat": now, "exp": now + expire_in, "iss": app_id},
        key=private_key_pem,
        algorithm="RS256",
    )
    return token


def create_jwt_headers(
    private_key_pem, app_id, expire_in=DEFAULT_JWT_TOKEN_EXPIRATION
):
    """Create an encrypted token for the specified App.

    :param bytes private_key_pem:
        The bytes of the private key for this GitHub Application.
    :param int app_id:
        The integer identifier for this GitHub Application.
    :param int expire_in:
        The length in seconds for this token to be valid for.
        Default: 600 seconds (10 minutes)
    :returns:
        Dictionary of headers for retrieving a token from a JWT.
    :rtype:
        dict
    """
    jwt_token = create_token(private_key_pem, app_id, expire_in)
    headers = {"Authorization": f"Bearer {jwt_token}"}
    headers.update(APP_PREVIEW_HEADERS)
    return headers
