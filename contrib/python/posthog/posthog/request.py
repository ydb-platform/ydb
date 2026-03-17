import json
import logging
import re
import socket
from dataclasses import dataclass
from datetime import date, datetime, timezone
from gzip import GzipFile
from io import BytesIO
from typing import Any, List, Optional, Tuple, Union

import requests
from dateutil.tz import tzutc
from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]
from urllib3.connection import HTTPConnection
from urllib3.util.retry import Retry

from posthog.utils import remove_trailing_slash
from posthog.version import VERSION

SocketOptions = List[Tuple[int, int, Union[int, bytes]]]

KEEPALIVE_IDLE_SECONDS = 60
KEEPALIVE_INTERVAL_SECONDS = 60
KEEPALIVE_PROBE_COUNT = 3

# TCP keepalive probes idle connections to prevent them from being dropped.
# SO_KEEPALIVE is cross-platform, but timing options vary:
# - Linux: TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT
# - macOS: only SO_KEEPALIVE (uses system defaults)
# - Windows: TCP_KEEPIDLE, TCP_KEEPINTVL (since Windows 10 1709)
KEEP_ALIVE_SOCKET_OPTIONS: SocketOptions = list(
    HTTPConnection.default_socket_options
) + [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
]
for attr, value in [
    ("TCP_KEEPIDLE", KEEPALIVE_IDLE_SECONDS),
    ("TCP_KEEPINTVL", KEEPALIVE_INTERVAL_SECONDS),
    ("TCP_KEEPCNT", KEEPALIVE_PROBE_COUNT),
]:
    if hasattr(socket, attr):
        KEEP_ALIVE_SOCKET_OPTIONS.append((socket.SOL_TCP, getattr(socket, attr), value))

# Status codes that indicate transient server errors worth retrying
RETRY_STATUS_FORCELIST = [408, 500, 502, 503, 504]


def _mask_tokens_in_url(url: str) -> str:
    """Mask token values in URLs for safe logging, keeping first 10 chars visible."""
    return re.sub(r"(token=)([^&]{10})[^&]*", r"\1\2...", url)


@dataclass
class GetResponse:
    """Response from a GET request with ETag support."""

    data: Any
    etag: Optional[str] = None
    not_modified: bool = False


class HTTPAdapterWithSocketOptions(HTTPAdapter):
    """HTTPAdapter with configurable socket options."""

    def __init__(self, *args, socket_options: Optional[SocketOptions] = None, **kwargs):
        self.socket_options = socket_options
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        if self.socket_options is not None:
            kwargs["socket_options"] = self.socket_options
        super().init_poolmanager(*args, **kwargs)


def _build_session(socket_options: Optional[SocketOptions] = None) -> requests.Session:
    """Build a session for general requests (batch, decide, etc.)."""
    adapter = HTTPAdapterWithSocketOptions(
        max_retries=Retry(
            total=2,
            connect=2,
            read=2,
        ),
        socket_options=socket_options,
    )
    session = requests.Session()
    session.mount("https://", adapter)
    return session


def _build_flags_session(
    socket_options: Optional[SocketOptions] = None,
) -> requests.Session:
    """
    Build a session for feature flag requests with POST retries.

    Feature flag requests are idempotent (read-only), so retrying POST
    requests is safe. This session retries on transient server errors
    (408, 5xx) and network failures with exponential backoff
    (0.5s, 1s delays between retries).
    """
    adapter = HTTPAdapterWithSocketOptions(
        max_retries=Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.5,
            status_forcelist=RETRY_STATUS_FORCELIST,
            allowed_methods=["POST"],
        ),
        socket_options=socket_options,
    )
    session = requests.Session()
    session.mount("https://", adapter)
    return session


_session = _build_session()
_flags_session = _build_flags_session()
_socket_options: Optional[SocketOptions] = None
_pooling_enabled = True


def _get_session() -> requests.Session:
    if _pooling_enabled:
        return _session
    return _build_session(_socket_options)


def _get_flags_session() -> requests.Session:
    if _pooling_enabled:
        return _flags_session
    return _build_flags_session(_socket_options)


def set_socket_options(socket_options: Optional[SocketOptions]) -> None:
    """
    Configure socket options for all HTTP connections.

    Example:
        from posthog import set_socket_options
        set_socket_options([(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)])
    """
    global _session, _flags_session, _socket_options
    if socket_options == _socket_options:
        return
    _socket_options = socket_options
    _session = _build_session(socket_options)
    _flags_session = _build_flags_session(socket_options)


def enable_keep_alive() -> None:
    """Enable TCP keepalive to prevent idle connections from being dropped."""
    set_socket_options(KEEP_ALIVE_SOCKET_OPTIONS)


def disable_connection_reuse() -> None:
    """Disable connection reuse, creating a fresh connection for each request."""
    global _pooling_enabled
    _pooling_enabled = False


US_INGESTION_ENDPOINT = "https://us.i.posthog.com"
EU_INGESTION_ENDPOINT = "https://eu.i.posthog.com"
DEFAULT_HOST = US_INGESTION_ENDPOINT
USER_AGENT = "posthog-python/" + VERSION


def determine_server_host(host: Optional[str]) -> str:
    """Determines the server host to use."""
    host_or_default = host or DEFAULT_HOST
    trimmed_host = remove_trailing_slash(host_or_default)
    if trimmed_host in ("https://app.posthog.com", "https://us.posthog.com"):
        return US_INGESTION_ENDPOINT
    elif trimmed_host == "https://eu.posthog.com":
        return EU_INGESTION_ENDPOINT
    else:
        return host_or_default


def post(
    api_key: str,
    host: Optional[str] = None,
    path=None,
    gzip: bool = False,
    timeout: int = 15,
    session: Optional[requests.Session] = None,
    **kwargs,
) -> requests.Response:
    """Post the `kwargs` to the API"""
    log = logging.getLogger("posthog")
    body = kwargs
    body["sentAt"] = datetime.now(tz=tzutc()).isoformat()
    url = remove_trailing_slash(host or DEFAULT_HOST) + path
    body["api_key"] = api_key
    data = json.dumps(body, cls=DatetimeSerializer)
    log.debug("making request: %s to url: %s", data, url)
    headers = {"Content-Type": "application/json", "User-Agent": USER_AGENT}
    if gzip:
        headers["Content-Encoding"] = "gzip"
        buf = BytesIO()
        with GzipFile(fileobj=buf, mode="w") as gz:
            # 'data' was produced by json.dumps(),
            # whose default encoding is utf-8.
            gz.write(data.encode("utf-8"))
        data = buf.getvalue()

    res = (session or _get_session()).post(
        url, data=data, headers=headers, timeout=timeout
    )

    if res.status_code == 200:
        log.debug("data uploaded successfully")

    return res


def _process_response(
    res: requests.Response, success_message: str, *, return_json: bool = True
) -> Union[requests.Response, Any]:
    log = logging.getLogger("posthog")
    if res.status_code == 200:
        log.debug(success_message)
        response = res.json() if return_json else res
        # Handle quota limited decide responses by raising a specific error
        # NB: other services also put entries into the quotaLimited key, but right now we only care about feature flags
        # since most of the other services handle quota limiting in other places in the application.
        if (
            isinstance(response, dict)
            and "quotaLimited" in response
            and isinstance(response["quotaLimited"], list)
            and "feature_flags" in response["quotaLimited"]
        ):
            log.warning(
                "[FEATURE FLAGS] PostHog feature flags quota limited, resetting feature flag data.  Learn more about billing limits at https://posthog.com/docs/billing/limits-alerts"
            )
            raise QuotaLimitError(res.status_code, "Feature flags quota limited")
        return response
    retry_after = None
    retry_after_header = res.headers.get("Retry-After")
    if retry_after_header:
        try:
            retry_after = float(retry_after_header)
        except (ValueError, TypeError):
            try:
                from email.utils import parsedate_to_datetime

                retry_after = max(
                    0.0,
                    (
                        parsedate_to_datetime(retry_after_header)
                        - datetime.now(timezone.utc)
                    ).total_seconds(),
                )
            except (ValueError, TypeError):
                pass

    try:
        payload = res.json()
        log.debug("received response: %s", payload)
        raise APIError(res.status_code, payload["detail"], retry_after=retry_after)
    except (KeyError, ValueError):
        raise APIError(res.status_code, res.text, retry_after=retry_after)


def decide(
    api_key: str,
    host: Optional[str] = None,
    gzip: bool = False,
    timeout: int = 15,
    **kwargs,
) -> Any:
    """Post the `kwargs to the decide API endpoint"""
    res = post(api_key, host, "/decide/?v=4", gzip, timeout, **kwargs)
    return _process_response(res, success_message="Feature flags decided successfully")


def flags(
    api_key: str,
    host: Optional[str] = None,
    gzip: bool = False,
    timeout: int = 15,
    **kwargs,
) -> Any:
    """Post the kwargs to the flags API endpoint with automatic retries."""
    res = post(
        api_key,
        host,
        "/flags/?v=2",
        gzip,
        timeout,
        session=_get_flags_session(),
        **kwargs,
    )
    return _process_response(
        res, success_message="Feature flags evaluated successfully"
    )


def remote_config(
    personal_api_key: str,
    project_api_key: str,
    host: Optional[str] = None,
    key: str = "",
    timeout: int = 15,
) -> Any:
    """Get remote config flag value from remote_config API endpoint"""
    response = get(
        personal_api_key,
        f"/api/projects/@current/feature_flags/{key}/remote_config?token={project_api_key}",
        host,
        timeout,
    )
    return response.data


def batch_post(
    api_key: str,
    host: Optional[str] = None,
    gzip: bool = False,
    timeout: int = 15,
    **kwargs,
) -> requests.Response:
    """Post the `kwargs` to the batch API endpoint for events"""
    res = post(api_key, host, "/batch/", gzip, timeout, **kwargs)
    return _process_response(
        res, success_message="data uploaded successfully", return_json=False
    )


def get(
    api_key: str,
    url: str,
    host: Optional[str] = None,
    timeout: Optional[int] = None,
    etag: Optional[str] = None,
) -> GetResponse:
    """
    Make a GET request with optional ETag support.

    If an etag is provided, sends If-None-Match header. Returns GetResponse with:
    - not_modified=True and data=None if server returns 304
    - not_modified=False and data=response if server returns 200
    """
    log = logging.getLogger("posthog")
    full_url = remove_trailing_slash(host or DEFAULT_HOST) + url
    headers = {"Authorization": "Bearer %s" % api_key, "User-Agent": USER_AGENT}

    if etag:
        headers["If-None-Match"] = etag

    res = _get_session().get(full_url, headers=headers, timeout=timeout)

    masked_url = _mask_tokens_in_url(full_url)

    # Handle 304 Not Modified
    if res.status_code == 304:
        log.debug(f"GET {masked_url} returned 304 Not Modified")
        response_etag = res.headers.get("ETag")
        return GetResponse(data=None, etag=response_etag or etag, not_modified=True)

    # Handle normal response
    data = _process_response(
        res, success_message=f"GET {masked_url} completed successfully"
    )
    response_etag = res.headers.get("ETag")
    return GetResponse(data=data, etag=response_etag, not_modified=False)


class APIError(Exception):
    def __init__(
        self, status: Union[int, str], message: str, retry_after: Optional[float] = None
    ):
        self.message = message
        self.status = status
        self.retry_after = retry_after

    def __str__(self):
        msg = "[PostHog] {0} ({1})"
        return msg.format(self.message, self.status)


class QuotaLimitError(APIError):
    pass


# Re-export requests exceptions for use in client.py
# This keeps all requests library imports centralized in this module
RequestsTimeout = requests.exceptions.Timeout
RequestsConnectionError = requests.exceptions.ConnectionError


class DatetimeSerializer(json.JSONEncoder):
    def default(self, obj: Any):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)
