# Copyright (c) 2016 Canonical Ltd
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import base64
import json
import os
import re
import socket
from enum import Enum
from typing import NamedTuple
from urllib import parse

import requests
import requests.adapters
import urllib3
import urllib3.connection
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from ws4py.client import WebSocketBaseClient

from pylxd import exceptions, managers

SNAP_ROOT = os.path.expanduser("~/snap/lxd/common/config/")
APT_ROOT = os.path.expanduser("~/.config/lxc/")
CERT_FILE_NAME = "client.crt"
KEY_FILE_NAME = "client.key"
# check that the cert file and key file exist at the appropriate path
if os.path.exists(os.path.join(SNAP_ROOT, CERT_FILE_NAME)):  # pragma: no cover
    CERTS_PATH = SNAP_ROOT  # pragma: no cover
else:  # pragma: no cover
    CERTS_PATH = APT_ROOT  # pragma: no cover


class Cert(NamedTuple):
    """Paths for a certificate/key pair."""

    cert: str
    key: str


DEFAULT_CERTS = Cert(
    cert=os.path.join(CERTS_PATH, CERT_FILE_NAME),
    key=os.path.join(CERTS_PATH, KEY_FILE_NAME),
)  # pragma: no cover

DEFAULT_SCHEME = "http+unix://"
SOCKET_CONNECTION_TIMEOUT = 60


class EventType(Enum):
    All = "all"
    Operation = "operation"
    Logging = "logging"
    Lifecycle = "lifecycle"


class _UnixSocketHTTPConnection(urllib3.connection.HTTPConnection):
    def __init__(self, unix_socket_url):
        super().__init__("localhost", timeout=SOCKET_CONNECTION_TIMEOUT)
        self.unix_socket_url = unix_socket_url
        self.timeout = SOCKET_CONNECTION_TIMEOUT
        self.sock = None

    def __del__(self):
        if self.sock:
            self.sock.close()

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        socket_path = parse.unquote(parse.urlparse(self.unix_socket_url).netloc)
        sock.connect(socket_path)
        self.sock = sock


class _UnixSocketHTTPConnectionPool(urllib3.HTTPConnectionPool):
    def __init__(self, socket_path):
        super().__init__("localhost")
        self.socket_path = socket_path

    def _new_conn(self):
        return _UnixSocketHTTPConnection(self.socket_path)


class _UnixAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, pool_connections=25, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pools = urllib3._collections.RecentlyUsedContainer(
            pool_connections, dispose_func=lambda p: p.close()
        )

    def get_connection(self, url, proxies):
        with self.pools.lock:
            conn = self.pools.get(url)
            if conn:
                return conn

            conn = _UnixSocketHTTPConnectionPool(url)
            self.pools[url] = conn

        return conn

    # This method is needed fo compatibility with later requests versions.
    def get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
        return self.get_connection(request.url, None)

    def request_url(self, request, proxies):
        return request.path_url

    def close(self):
        self.pools.clear()


class LXDSSLAdapter(requests.adapters.HTTPAdapter):
    def cert_verify(self, conn, url, verify, cert):
        with open(verify, "rb") as fd:
            servercert = x509.load_pem_x509_certificate(fd.read())
            fingerprint = servercert.fingerprint(hashes.SHA256())

        conn.assert_fingerprint = "".join([f"{i:02x}" for i in fingerprint])
        super().cert_verify(conn, url, False, cert)


def get_session_for_url(url: str, verify=None, cert=None) -> requests.Session:
    """Create a Session for use with requests for the given URL.

    Call sites can use this to customise the session before passing into a Client.
    """
    session = requests.Session()
    if url.startswith(DEFAULT_SCHEME):
        session.mount(DEFAULT_SCHEME, _UnixAdapter())
    else:
        session.cert = cert
        session.verify = verify

        if isinstance(verify, str):
            session.mount(url, LXDSSLAdapter())
    return session


class _APINode:
    """An api node object.

    `session` is a `requests.Session` which is used for issuing
       requests. See `get_session_for_url`
    """

    def __init__(
        self,
        api_endpoint,
        session,
        timeout=None,
        project=None,
    ):
        self._api_endpoint = api_endpoint
        self._timeout = timeout
        self._project = project
        self.session = session

    def __getattr__(self, name):
        """Converts attribute lookup into the next /<segment> of an api
        url.

        :param name: the next segment
        :type name: str
        :returns: new _APINode with /<name> on the end
        :rtype: _APINode
        """
        # '-' can't be used in variable names
        if name in ("storage_pools", "virtual_machines"):
            name = name.replace("_", "-")
        return self.__class__(
            f"{self._api_endpoint}/{name}",
            session=self.session,
            timeout=self._timeout,
            project=self._project,
        )

    def __getitem__(self, item):
        """This converts python api.thing[name] -> ".../thing/name"

        :param item: the 'thing' in the square-braces in a python expr.
        :type item: str
        :returns: A new _APINode(with the new item tagged on as /<item>
        :rtype: _APINode
        """
        return self.__class__(
            f"{self._api_endpoint}/{item}",
            session=self.session,
            timeout=self._timeout,
            project=self._project,
        )

    def _assert_response(
        self, response, allowed_status_codes=(200,), stream=False, is_api=True
    ):
        """Assert properties of the response.

        LXD's API clearly defines specific responses. If the API
        response is something unexpected (i.e. an error), then
        we need to raise an exception and have the call points
        handle the errors or just let the issue be raised to the
        user.
        """
        if response.status_code not in allowed_status_codes:
            if response.status_code == 404:
                raise exceptions.NotFound(response)
            elif response.status_code == 409:
                raise exceptions.Conflict(response)
            raise exceptions.LXDAPIException(response)

        # In the case of streaming, we can't validate the json the way we
        # would with normal HTTP responses, so just ignore that entirely.
        # Likewize, we can ignore NON api calls as the contents don't need to
        # be validated.
        if stream or not is_api:
            return

        try:
            data = response.json()
        except ValueError:
            # Not a JSON response
            return

        if response.status_code == 200:
            # Synchronous request
            try:
                if data["type"] != "sync":
                    raise exceptions.LXDAPIException(response)
            except KeyError:
                # Missing 'type' in response
                raise exceptions.LXDAPIException(response)

    @property
    def scheme(self):
        return parse.urlparse(self.api._api_endpoint).scheme

    @property
    def netloc(self):
        return parse.urlparse(self.api._api_endpoint).netloc

    def get(self, *args, **kwargs):
        """Perform an HTTP GET.

        Note if 'is_api' is passed in the kwargs then it is popped and used to
        determine whether the get is an API call or a raw call.
        This is for py27 compatibility.
        """
        is_api = kwargs.pop("is_api", True)
        kwargs["timeout"] = kwargs.get("timeout", self._timeout)

        if self._project is not None:
            params = kwargs.get("params", {})
            params["project"] = self._project
            kwargs["params"] = params

        response = self.session.get(self._api_endpoint, *args, **kwargs)
        self._assert_response(
            response, stream=kwargs.get("stream", False), is_api=is_api
        )
        return response

    def post(self, *args, **kwargs):
        """Perform an HTTP POST."""
        kwargs["timeout"] = kwargs.get("timeout", self._timeout)

        target = kwargs.pop("target", None)
        if target is not None:
            params = kwargs.get("params", {})
            params["target"] = target
            kwargs["params"] = params

        if self._project is not None:
            params = kwargs.get("params", {})
            params["project"] = self._project
            kwargs["params"] = params

        response = self.session.post(self._api_endpoint, *args, **kwargs)
        # Prior to LXD 2.0.3, successful synchronous requests returned 200,
        # rather than 201.
        self._assert_response(response, allowed_status_codes=(200, 201, 202))
        return response

    def put(self, *args, **kwargs):
        """Perform an HTTP PUT."""
        kwargs["timeout"] = kwargs.get("timeout", self._timeout)

        if self._project is not None:
            params = kwargs.get("params", {})
            params["project"] = self._project
            kwargs["params"] = params

        response = self.session.put(self._api_endpoint, *args, **kwargs)
        self._assert_response(response, allowed_status_codes=(200, 202))
        return response

    def patch(self, *args, **kwargs):
        """Perform an HTTP PATCH."""
        kwargs["timeout"] = kwargs.get("timeout", self._timeout)

        if self._project is not None:
            params = kwargs.get("params", {})
            params["project"] = self._project
            kwargs["params"] = params

        response = self.session.patch(self._api_endpoint, *args, **kwargs)
        self._assert_response(response, allowed_status_codes=(200, 202))
        return response

    def delete(self, *args, **kwargs):
        """Perform an HTTP delete."""
        kwargs["timeout"] = kwargs.get("timeout", self._timeout)

        if self._project is not None:
            params = kwargs.get("params", {})
            params["project"] = self._project
            kwargs["params"] = params

        response = self.session.delete(self._api_endpoint, *args, **kwargs)
        self._assert_response(response, allowed_status_codes=(200, 202))
        return response


class _WebsocketClient(WebSocketBaseClient):
    """A basic websocket client for the LXD API.

    This client is intentionally barebones, and serves
    as a simple default. It simply connects and saves
    all json messages to a messages attribute, which can
    then be read are parsed.
    """

    def handshake_ok(self):
        self.messages = []

    def received_message(self, message):
        json_message = json.loads(message.data.decode("utf-8"))
        self.messages.append(json_message)


# Helper function used by Client.authenticate()
def _is_a_token(secret):
    """Inspect the provided secret to determine if it is a trust token.

    Try to base64 decode and parse the JSON to see if it contains a "secret" key.

    :param secret: The secret to inspect
    :type secret: str
    :returns: True if the secret is a trust token

    >>> _is_a_token("password")
    False
    >>> _is_a_token(base64.b64encode("password".encode("utf-8")))
    False
    >>> token = '{"client_name":"foo","fingerprint":"abcd","addresses":["192.0.2.1:8443"],"secret":"I-am-a-secret","expires_at":"0001-01-01T00:00:00Z","type":""}'
    >>> _is_a_token(base64.b64encode(json.dumps(token).encode("utf-8")))
    True
    """
    try:
        b64 = base64.b64decode(secret)
        token = json.loads(b64.decode("utf-8"))
        return "secret" in token
    except (TypeError, ValueError, json.JSONDecodeError, base64.binascii.Error):
        return False


class Client:
    """Client class for LXD REST API.

    This client wraps all the functionality required to interact with
    LXD, and is meant to be the sole entry point.

    .. attribute:: instances

        A :class:`models.Instance <pylxd.models.Instance>`.

    .. attribute:: containers

        A :class:`models.Container <pylxd.models.Container>`.

    .. attribute:: virtual_machines

        A :class:`models.VirtualMachine <pylxd.models.VirtualMachine>`.

    .. attribute:: images

        A :class:`models.Image <pylxd.models.Image>`.

    .. attribute:: operations

        A :class:`models.Operation <pylxd.models.Operation>`.

    .. attribute:: profiles

        A :class:`models.Profile <pylxd.models.Profile>`.

    .. attribute:: projects

        A :class:`models.Project <pylxd.models.Project>`.

    .. attribute:: api

        This attribute provides tree traversal syntax to LXD's REST API for
        lower-level interaction.

        Use the name of the url part as attribute or item of an api object to
        create another api object appended with the new url part name, ie:

            # /
            >> api = Client().api
            >> response = api.get()
            # Check status code and response
            >> print(response.status_code, response.json())
            # /instances/test/
            >> print(api.instances['test'].get().json())
    """

    def __init__(
        self,
        endpoint=None,
        version="1.0",
        cert=None,
        verify=True,
        timeout=None,
        project=None,
        session=None,
    ):
        """Constructs a LXD client

        :param endpoint: (optional): endpoint can be an http endpoint or
            a path to a unix socket.
        :param version: (optional): API version string to use with LXD
        :param cert: (optional): A tuple of (cert, key) to use with
            the http socket for client authentication
        :param verify: (optional): Either a boolean, in which case it controls
            whether we verify the server's TLS certificate, or a string, in
            which case it must be a path to a CA bundle to use.
            Defaults to ``True``.
        :param timeout: (optional) How long to wait for the server to send
            data before giving up, as a float, or a :ref:`(connect timeout,
            read timeout) <timeouts>` tuple.
        :param project: (optional) Name of the LXD project to interact with.
        :param session: (optional) A requests.Session to use for
            interactions with the endpoint. If given, cert and verify
            arguments are ignored - see get_session_for_url().
        """

        self.project = project
        if endpoint:
            if endpoint.startswith("/") and os.path.exists(endpoint):
                endpoint = f"http+unix://{parse.quote(endpoint, safe='')}"
            else:
                # Extra trailing slashes cause LXD to 301
                endpoint = endpoint.rstrip("/")
                if cert is None and (
                    os.path.exists(DEFAULT_CERTS.cert)
                    and os.path.exists(DEFAULT_CERTS.key)
                ):
                    cert = DEFAULT_CERTS

                # Try to use an existing server certificate if one exists
                if isinstance(verify, bool) and endpoint.startswith("https://"):
                    no_proto = re.sub(r"^https://\[?", "", endpoint)
                    remote = re.sub(r"]?(:[0-9]+)?$", "", no_proto)
                    remote_cert_path = os.path.join(
                        CERTS_PATH, "servercerts", remote + ".crt"
                    )
                    if os.path.exists(remote_cert_path):
                        verify = remote_cert_path
        else:
            if "LXD_DIR" in os.environ:
                path = os.path.join(os.environ.get("LXD_DIR"), "unix.socket")
            elif os.path.exists("/var/snap/lxd/common/lxd/unix.socket"):
                path = "/var/snap/lxd/common/lxd/unix.socket"
            else:
                path = "/var/lib/lxd/unix.socket"
            endpoint = f"http+unix://{parse.quote(path, safe='')}"
        self.cert = cert
        if session is None:
            session = get_session_for_url(endpoint, cert=cert, verify=verify)
        self.api = _APINode(
            f"{endpoint}/{version}", session, timeout=timeout, project=project
        )
        use_ssl = self.api.scheme == "https" and self.cert
        self.ssl_options = (
            {"certfile": self.cert[0], "keyfile": self.cert[1]} if use_ssl else None
        )

        # Verify the connection is valid.
        try:
            response = self.api.get()
            if response.status_code != 200:
                raise exceptions.ClientConnectionFailed()
            self.host_info = response.json()["metadata"]

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.InvalidURL,
        ) as e:
            raise exceptions.ClientConnectionFailed(str(e))

        if self.project not in (None, "default") and not self.has_api_extension(
            "projects"
        ):
            raise exceptions.ClientConnectionFailed(
                "Remote server doesn't handle projects"
            )

        self.cluster = managers.ClusterManager(self)
        self.certificates = managers.CertificateManager(self)
        self.instances = managers.InstanceManager(self)
        self.containers = managers.ContainerManager(self)
        self.virtual_machines = managers.VirtualMachineManager(self)
        self.images = managers.ImageManager(self)
        self.networks = managers.NetworkManager(self)
        self.operations = managers.OperationManager(self)
        self.profiles = managers.ProfileManager(self)
        self.projects = managers.ProjectManager(self)
        self.storage_pools = managers.StoragePoolManager(self)
        self._resource_cache = None

    @property
    def trusted(self):
        return self.host_info["auth"] == "trusted"

    @property
    def server_clustered(self):
        return self.host_info["environment"].get("server_clustered", False)

    @property
    def resources(self):
        if self._resource_cache is None:
            self.assert_has_api_extension("resources")
            response = self.api.resources.get()
            if response.status_code != 200:
                raise exceptions.ClientConnectionFailed()
            self._resource_cache = response.json()["metadata"]
        return self._resource_cache

    def has_api_extension(self, name):
        """Return True if the `name` api extension exists.

        :param name: the api_extension to look for.
        :type name: str
        :returns: True if extension exists
        :rtype: bool
        """
        return name in self.host_info["api_extensions"]

    def assert_has_api_extension(self, name):
        """Asserts that the `name` api_extension exists.
        If not, then is raises the LXDAPIExtensionNotAvailable error.

        :param name: the api_extension to test for
        :type name: str
        :returns: None
        :raises: :class:`pylxd.exceptions.LXDAPIExtensionNotAvailable`
        """
        if not self.has_api_extension(name):
            raise exceptions.LXDAPIExtensionNotAvailable(name)

    def authenticate(self, secret, use_token_auth=True):
        if self.trusted:
            return

        if not isinstance(self.api.session.cert, tuple):
            raise exceptions.ClientConnectionFailed("No client certificate specified")

        try:
            with open(self.api.session.cert[0]) as f:
                cert = f.read().encode("utf-8")
        except FileNotFoundError:
            raise exceptions.ClientConnectionFailed("Client certificate not found")

        # Quirk to handle 5.21 that supports explicit trust tokens as well as
        # password auth. We need to ascertain if the provided secret is indeed a
        # token before trying to use it as such.
        secret_is_a_token = False
        if use_token_auth and self.has_api_extension("explicit_trust_token"):
            secret_is_a_token = _is_a_token(secret)

        if secret_is_a_token:
            self.certificates.create(password="", cert_data=cert, secret=secret)
        else:
            self.certificates.create(password=secret, cert_data=cert)

        # Refresh the host info
        response = self.api.get()
        self.host_info = response.json()["metadata"]

    @property
    def websocket_url(self):
        if self.api.scheme in ("http", "https"):
            host = self.api.netloc
            if self.api.scheme == "http":
                scheme = "ws"
            else:
                scheme = "wss"
        else:
            scheme = "ws+unix"
            host = parse.unquote(self.api.netloc)
        url = parse.urlunparse((scheme, host, "", "", "", ""))
        return url

    def events(self, websocket_client=None, event_types=None):
        """Get a websocket client for getting events.

        /events is a websocket url, and so must be handled differently than
        most other LXD API endpoints. This method returns
        a client that can be interacted with like any
        regular python socket.

        An optional `websocket_client` parameter can be
        specified for implementation-specific handling
        of events as they occur.

        :param websocket_client: Optional websocket client can be specified for
         implementation-specific handling of events as they occur.
        :type websocket_client: ws4py.client import WebSocketBaseClient

        :param event_types: Optional set of event types to propagate. Omit this
         argument or specify {EventTypes.All} to receive all events.
        :type event_types: Set[EventType]

        :returns: instance of the websocket client
        :rtype: Option[_WebsocketClient(), `websocket_client`]
        """
        if websocket_client is None:
            websocket_client = _WebsocketClient

        client = websocket_client(self.websocket_url, ssl_options=self.ssl_options)
        parsed = parse.urlparse(self.api.events._api_endpoint)

        resource = parsed.path

        if event_types and EventType.All not in event_types:
            query = parse.parse_qs(parsed.query)
            query.update({"type": ",".join(t.value for t in event_types)})
            resource = f"{resource}?{parse.urlencode(query)}"

        client.resource = resource

        return client
