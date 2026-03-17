from __future__ import annotations

import platform
import re
import sys
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict, cast
from urllib.parse import urljoin

import requests
from rich.console import Console
from rich.prompt import IntPrompt
from typing_extensions import Self

from logfire.exceptions import LogfireConfigError

from .utils import UnexpectedResponse, read_toml_file

HOME_LOGFIRE = Path.home() / '.logfire'
"""Folder used to store global configuration, and user tokens."""
DEFAULT_FILE = HOME_LOGFIRE / 'default.toml'
"""File used to store user tokens."""


PYDANTIC_LOGFIRE_TOKEN_PATTERN = re.compile(
    r'^(?P<safe_part>pylf_v(?P<version>[0-9]+)_(?P<region>[a-z]+)_)(?P<token>[a-zA-Z0-9]+)$'
)


class _RegionData(TypedDict):
    base_url: str
    gcp_region: str


REGIONS: dict[str, _RegionData] = {
    'us': {
        'base_url': 'https://logfire-us.pydantic.dev',
        'gcp_region': 'us-east4',
    },
    'eu': {
        'base_url': 'https://logfire-eu.pydantic.dev',
        'gcp_region': 'europe-west4',
    },
}
"""The existing Logfire regions."""


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
    def from_user_token_data(cls, base_url: str, token: UserTokenData) -> Self:
        return cls(
            token=token['token'],
            base_url=base_url,
            expiration=token['expiration'],
        )

    @property
    def is_expired(self) -> bool:
        """Whether the token is expired."""
        return datetime.now(tz=timezone.utc) >= datetime.fromisoformat(self.expiration.rstrip('Z')).replace(
            tzinfo=timezone.utc
        )

    def __str__(self) -> str:
        region = 'us'
        if match := PYDANTIC_LOGFIRE_TOKEN_PATTERN.match(self.token):
            region = match.group('region')
            if region not in REGIONS:
                region = 'us'

        token_repr = f'{region.upper()} ({self.base_url}) - '
        if match:
            token_repr += match.group('safe_part') + match.group('token')[:5]
        else:
            token_repr += self.token[:5]
        token_repr += '****'
        return token_repr


@dataclass
class UserTokenCollection:
    """A collection of user tokens, read from a user tokens file.

    Args:
        path: The path where the user tokens will be stored. If the path doesn't exist,
            an empty collection is created. Defaults to `~/.logfire/default.toml`.
    """

    user_tokens: dict[str, UserToken]
    """A mapping between base URLs and user tokens."""

    path: Path
    """The path where the user tokens are stored."""

    def __init__(self, path: Path | None = None) -> None:
        # FIXME: we can't set the default value of `path` to `DEFAULT_FILE`, otherwise
        # `mock.patch()` doesn't work:
        self.path = path if path is not None else DEFAULT_FILE
        try:
            data = cast(UserTokensFileData, read_toml_file(self.path))
        except FileNotFoundError:
            data: UserTokensFileData = {}
        self.user_tokens = {url: UserToken(base_url=url, **token) for url, token in data.get('tokens', {}).items()}

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
        tokens_list = list(self.user_tokens.values())

        if base_url is not None:
            token = self.user_tokens.get(base_url)
            if token is None:
                raise LogfireConfigError(
                    f'No user token was found matching the {base_url} Logfire URL. '
                    'Please run `logfire auth` to authenticate.'
                )
        elif len(tokens_list) == 1:
            token = tokens_list[0]
        elif len(tokens_list) >= 2:
            choices_str = '\n'.join(
                f'{i}. {token} ({"expired" if token.is_expired else "valid"})'
                for i, token in enumerate(tokens_list, start=1)
            )
            int_choice = IntPrompt.ask(
                f'Multiple user tokens found. Please select one:\n{choices_str}\n',
                choices=[str(i) for i in range(1, len(tokens_list) + 1)],
                console=Console(file=sys.stderr),
            )
            token = tokens_list[int_choice - 1]
        else:  # tokens_list == []
            raise LogfireConfigError('You are not logged into Logfire. Please run `logfire auth` to authenticate.')

        if token.is_expired:
            raise LogfireConfigError(f'User token {token} is expired. Please run `logfire auth` to authenticate.')
        return token

    def is_logged_in(self, base_url: str | None = None) -> bool:
        """Check whether the user token collection contains at least one valid user token.

        Args:
            base_url: Only check for user tokens valid for this base URL. If not provided,
                all the tokens of the collection will be considered.
        """
        if base_url is not None:
            tokens = (t for t in self.user_tokens.values() if t.base_url == base_url)
        else:
            tokens = self.user_tokens.values()
        return any(not t.is_expired for t in tokens)

    def add_token(self, base_url: str, token: UserTokenData) -> UserToken:
        """Add a user token to the collection."""
        self.user_tokens[base_url] = user_token = UserToken.from_user_token_data(base_url, token)
        self._dump()
        return user_token

    def _dump(self) -> None:
        """Dump the user token collection as TOML to the provided path."""
        # There's no standard library package to write TOML files, so we'll write it manually.
        with self.path.open('w') as f:
            for base_url, user_token in self.user_tokens.items():
                f.write(f'[tokens."{base_url}"]\n')
                f.write(f'token = "{user_token.token}"\n')
                f.write(f'expiration = "{user_token.expiration}"\n')


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
    machine_name = platform.uname()[1]
    device_auth_endpoint = urljoin(base_api_url, '/v1/device-auth/new/')
    try:
        res = session.post(device_auth_endpoint, params={'machine_name': machine_name})
        UnexpectedResponse.raise_for_status(res)
    except requests.RequestException as e:  # pragma: no cover
        raise LogfireConfigError('Failed to request a device code.') from e
    data: NewDeviceFlow = res.json()
    return data['device_code'], data['frontend_auth_url']


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
    auth_endpoint = urljoin(base_api_url, f'/v1/device-auth/wait/{device_code}')
    errors = 0
    while True:
        try:
            res = session.get(auth_endpoint, timeout=15)
            UnexpectedResponse.raise_for_status(res)
        except requests.RequestException as e:
            errors += 1
            if errors >= 4:
                raise LogfireConfigError('Failed to poll for token.') from e
            warnings.warn('Failed to poll for token. Retrying...')
        else:
            opt_user_token: UserTokenData | None = res.json()
            if opt_user_token:
                return opt_user_token
