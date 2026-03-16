import requests
from .utils import UnexpectedResponse as UnexpectedResponse, read_toml_file as read_toml_file
from _typeshed import Incomplete
from dataclasses import dataclass
from logfire.exceptions import LogfireConfigError as LogfireConfigError
from pathlib import Path
from typing import TypedDict
from typing_extensions import Self

HOME_LOGFIRE: Incomplete
DEFAULT_FILE: Incomplete
PYDANTIC_LOGFIRE_TOKEN_PATTERN: Incomplete

class _RegionData(TypedDict):
    base_url: str
    gcp_region: str

REGIONS: dict[str, _RegionData]

class UserTokenData(TypedDict):
    """User token data."""
    token: str
    expiration: str

class UserTokensFileData(TypedDict, total=False):
    """Content of the file containing the user tokens."""
    tokens: dict[str, UserTokenData]

@dataclass
class UserToken:
    """A user token."""
    token: str
    base_url: str
    expiration: str
    @classmethod
    def from_user_token_data(cls, base_url: str, token: UserTokenData) -> Self: ...
    @property
    def is_expired(self) -> bool:
        """Whether the token is expired."""

@dataclass
class UserTokenCollection:
    """A collection of user tokens, read from a user tokens file.

    Args:
        path: The path where the user tokens will be stored. If the path doesn't exist,
            an empty collection is created. Defaults to `~/.logfire/default.toml`.
    """
    user_tokens: dict[str, UserToken]
    path: Path
    def __init__(self, path: Path | None = None) -> None: ...
    def get_token(self, base_url: str | None = None) -> UserToken:
        """Get a user token from the collection.

        Args:
            base_url: Only look for user tokens valid for this base URL. If not provided,
                all the tokens of the collection will be considered: if only one token is
                available, it will be used, otherwise the user will be prompted to choose
                a token.

        Raises:
            LogfireConfigError: If no user token is found (no token matched the base URL,
                the collection is empty, or the selected token is expired).
        """
    def is_logged_in(self, base_url: str | None = None) -> bool:
        """Check whether the user token collection contains at least one valid user token.

        Args:
            base_url: Only check for user tokens valid for this base URL. If not provided,
                all the tokens of the collection will be considered.
        """
    def add_token(self, base_url: str, token: UserTokenData) -> UserToken:
        """Add a user token to the collection."""

class NewDeviceFlow(TypedDict):
    """Matches model of the same name in the backend."""
    device_code: str
    frontend_auth_url: str

def request_device_code(session: requests.Session, base_api_url: str) -> tuple[str, str]:
    """Request a device code from the Logfire API.

    Args:
        session: The `requests` session to use.
        base_api_url: The base URL of the Logfire instance.

    Returns:
    return data['device_code'], data['frontend_auth_url']
        The device code and the frontend URL to authenticate the device at, as a `NewDeviceFlow` dict.
    """
def poll_for_token(session: requests.Session, device_code: str, base_api_url: str) -> UserTokenData:
    """Poll the Logfire API for the user token.

    This function will keep polling the API until it receives a user token, not that
    each request should take ~10 seconds as the API endpoint will block waiting for the user to
    complete authentication.

    Args:
        session: The `requests` session to use.
        device_code: The device code to poll for.
        base_api_url: The base URL of the Logfire instance.

    Returns:
        The user token.
    """
