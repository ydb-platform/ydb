"""Module containing session and auth logic."""
import collections.abc as abc_collections
import datetime
from contextlib import contextmanager
from logging import getLogger

import dateutil.parser
import requests

from . import __version__
from . import exceptions as exc

__url_cache__ = {}
__logs__ = getLogger(__package__)


def requires_2fa(response):
    """Determine whether a response requires us to prompt the user for 2FA."""
    if (
        response.status_code == 401
        and "X-GitHub-OTP" in response.headers
        and "required" in response.headers["X-GitHub-OTP"]
    ):
        return True
    return False


class BasicAuth(requests.auth.HTTPBasicAuth):
    """Sub-class requests's class so we have a nice repr."""

    def __repr__(self):
        """Use the username as the representation."""
        return f"basic {self.username}"


class TokenAuth(requests.auth.AuthBase):
    """Auth class that handles simple tokens."""

    header_format_str = "token {}"

    def __init__(self, token):
        """Store our token."""
        self.token = token

    def __repr__(self):
        """Return a nice view of the token in use."""
        return f"token {self.token[:4]}..."

    def __ne__(self, other):
        """Test for equality, or the lack thereof."""
        return not self == other

    def __eq__(self, other):
        """Test for equality, or the lack thereof."""
        return self.token == getattr(other, "token", None)

    def __call__(self, request):
        """Add the authorization header and format it."""
        request.headers["Authorization"] = self.header_format_str.format(
            self.token
        )
        return request


class GitHubSession(requests.Session):
    """Our slightly specialized Session object.

    Normally this is created automatically by
    :class:`~github3.github.GitHub`.  To use alternate values for
    network timeouts, this class can be instantiated directly and
    passed to the GitHub object.  For example:

    .. code-block:: python

       gh = github.GitHub(session=session.GitHubSession(
           default_connect_timeout=T, default_read_timeout=N))

    :param default_connect_timeout:
       the number of seconds to wait when establishing a connection to
       GitHub
    :type default_connect_timeout:
       float
    :param default_read_timeout:
       the number of seconds to wait for a response from GitHub
    :type default_read_timeout:
       float
    """

    auth = None
    __attrs__ = requests.Session.__attrs__ + [
        "base_url",
        "two_factor_auth_cb",
        "default_connect_timeout",
        "default_read_timeout",
        "request_counter",
    ]

    def __init__(self, default_connect_timeout=4, default_read_timeout=10):
        """Slightly modify how we initialize our session."""
        super().__init__()
        self.default_connect_timeout = default_connect_timeout
        self.default_read_timeout = default_read_timeout
        self.headers.update(
            {
                # Only accept JSON responses
                "Accept": "application/vnd.github.v3.full+json",
                # Only accept UTF-8 encoded data
                "Accept-Charset": "utf-8",
                # Always sending JSON
                "Content-Type": "application/json",
                # Set our own custom User-Agent string
                "User-Agent": f"github3.py/{__version__}",
            }
        )
        self.base_url = "https://api.github.com"
        self.two_factor_auth_cb = None
        self.request_counter = 0

    @property
    def timeout(self):
        """Return the timeout tuple as expected by Requests"""
        return (self.default_connect_timeout, self.default_read_timeout)

    def basic_auth(self, username, password):
        """Set the Basic Auth credentials on this Session.

        :param str username: Your GitHub username
        :param str password: Your GitHub password
        """
        if not (username and password):
            return

        self.auth = BasicAuth(username, password)

    def build_url(self, *args, **kwargs):
        """Build a new API url from scratch."""
        parts = [kwargs.get("base_url") or self.base_url]
        parts.extend(args)
        parts = [str(p) for p in parts]
        key = tuple(parts)
        __logs__.info("Building a url from %s", key)
        if key not in __url_cache__:
            __logs__.info("Missed the cache building the url")
            __url_cache__[key] = "/".join(parts)
        return __url_cache__[key]

    def handle_two_factor_auth(self, args, kwargs):
        """Handler for when the user has 2FA turned on."""
        headers = kwargs.pop("headers", {})
        headers.update({"X-GitHub-OTP": str(self.two_factor_auth_cb())})
        kwargs.update(headers=headers)
        return super().request(*args, **kwargs)

    def has_auth(self):
        """Check for whether or not the user has authentication configured."""
        return self.auth or self.headers.get("Authorization")

    def oauth2_auth(self, client_id, client_secret):
        """Use OAuth2 for authentication.

        It is suggested you install requests-oauthlib to use this.

        :param str client_id: Client ID retrieved from GitHub
        :param str client_secret: Client secret retrieved from GitHub
        """
        raise NotImplementedError("These features are not implemented yet")

    def request(self, *args, **kwargs):
        """Make a request, count it, and handle 2FA if necessary."""
        kwargs.setdefault("timeout", self.timeout)
        response = super().request(*args, **kwargs)
        self.request_counter += 1
        if requires_2fa(response) and self.two_factor_auth_cb:
            # No need to flatten and re-collect the args in
            # handle_two_factor_auth
            new_response = self.handle_two_factor_auth(args, kwargs)
            new_response.history.append(response)
            response = new_response
        return response

    def retrieve_client_credentials(self):
        """Return the client credentials.

        :returns: tuple(client_id, client_secret)
        """
        client_id = self.params.get("client_id")
        client_secret = self.params.get("client_secret")
        return (client_id, client_secret)

    def two_factor_auth_callback(self, callback):
        """Register our 2FA callback specified by the user."""
        if not callback:
            return

        if not isinstance(callback, abc_collections.Callable):
            raise ValueError("Your callback should be callable")

        self.two_factor_auth_cb = callback

    def token_auth(self, token):
        """Use an application token for authentication.

        :param str token: Application token retrieved from GitHub's
            /authorizations endpoint
        """
        if not token:
            return

        self.auth = TokenAuth(token)

    def app_bearer_token_auth(self, headers, expire_in):
        """Authenticate as an App to be able to view its metadata."""
        if not headers:
            return

        self.auth = AppBearerTokenAuth(headers, expire_in)

    def app_installation_token_auth(self, json):
        """Use an access token generated by an App's installation."""
        if not json:
            return

        self.auth = AppInstallationTokenAuth(
            json["token"], json["expires_at"]
        )

    @contextmanager
    def temporary_basic_auth(self, *auth):
        """Allow us to temporarily swap out basic auth credentials."""
        old_basic_auth = self.auth
        old_token_auth = self.headers.get("Authorization")

        self.basic_auth(*auth)
        yield

        self.auth = old_basic_auth
        if old_token_auth:
            self.headers["Authorization"] = old_token_auth

    @contextmanager
    def no_auth(self):
        """Unset authentication temporarily as a context manager."""
        old_basic_auth, self.auth = self.auth, None
        old_token_auth = self.headers.pop("Authorization", None)

        yield

        self.auth = old_basic_auth
        if old_token_auth:
            self.headers["Authorization"] = old_token_auth


def _utcnow():
    return datetime.datetime.now(dateutil.tz.UTC)


class AppInstallationTokenAuth(TokenAuth):
    """Use token authentication but throw an exception on expiration."""

    def __init__(self, token, expires_at):
        """Set-up our authentication handler."""
        super().__init__(token)
        self.expires_at_str = expires_at
        self.expires_at = dateutil.parser.parse(expires_at)

    def __repr__(self):
        """Return a nice view of the token in use."""
        return "app installation token {}... expiring at {}".format(
            self.token[:4], self.expires_at_str
        )

    @property
    def expired(self):
        """Indicate whether our token is expired or not."""
        now = _utcnow()
        return now > self.expires_at

    def __call__(self, request):
        """Add the authorization header and format it."""
        if self.expired:
            raise exc.AppInstallationTokenExpired(
                "Your app installation token expired at {}".format(
                    self.expires_at_str
                )
            )
        return super().__call__(request)


class AppBearerTokenAuth(TokenAuth):
    """Use JWT authentication but throw an exception on expiration."""

    header_format_str = "Bearer {}"

    def __init__(self, token, expire_in):
        """Set-up our authentication handler."""
        super().__init__(token)
        expire_in = datetime.timedelta(seconds=expire_in)
        self.expires_at = _utcnow() + expire_in

    def __repr__(self):
        """Return a helpful view of the token."""
        return "app bearer token {} expiring at {}".format(
            self.token[:4], str(self.expires_at)
        )

    @property
    def expired(self):
        """Indicate whether our token is expired or not."""
        now = _utcnow()
        return now > self.expires_at

    def __call__(self, request):
        """Add the authorization header and format it."""
        if self.expired:
            raise exc.AppTokenExpired(
                f"Your app token expired at {str(self.expires_at)}"
            )
        return super().__call__(request)
