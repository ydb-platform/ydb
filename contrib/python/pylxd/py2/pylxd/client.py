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
import json
import os
import os.path
from collections import namedtuple
from enum import Enum

import requests
import requests_unixsocket
from six.moves.urllib import parse
try:
    from ws4py.client import WebSocketBaseClient
    _ws4py_installed = True
except ImportError:  # pragma: no cover
    WebSocketBaseClient = object
    _ws4py_installed = False

from pylxd import exceptions, managers

requests_unixsocket.monkeypatch()

LXD_PATH = '.config/lxc/'
SNAP_ROOT = os.path.expanduser('~/snap/lxd/current/')
APT_ROOT = os.path.expanduser('~/')
CERT_FILE_NAME = 'client.crt'
KEY_FILE_NAME = 'client.key'
# check that the cert file and key file exist at the appopriate path
if os.path.exists(os.path.join(
        SNAP_ROOT, LXD_PATH, CERT_FILE_NAME)):  # pragma: no cover
    CERTS_PATH = os.path.join(SNAP_ROOT, LXD_PATH)  # pragma: no cover
else:  # pragma: no cover
    CERTS_PATH = os.path.join(APT_ROOT, LXD_PATH)  # pragma: no cover

Cert = namedtuple('Cert', ['cert', 'key'])  # pragma: no cover
DEFAULT_CERTS = Cert(
    cert=os.path.expanduser(os.path.join(CERTS_PATH, CERT_FILE_NAME)),
    key=os.path.expanduser(os.path.join(CERTS_PATH, KEY_FILE_NAME))
)  # pragma: no cover


class EventType(Enum):
    All = 'all'
    Operation = 'operation'
    Logging = 'logging'
    Lifecycle = 'lifecycle'


class _APINode(object):
    """An api node object."""

    def __init__(self, api_endpoint, cert=None, verify=True, timeout=None):
        self._api_endpoint = api_endpoint
        self._timeout = timeout

        if self._api_endpoint.startswith('http+unix://'):
            self.session = requests_unixsocket.Session()
        else:
            self.session = requests.Session()
            self.session.cert = cert
            self.session.verify = verify

    def __getattr__(self, name):
        """Converts attribute lookup into the next /<segment> of an api
        url.

        :param name: the next segment
        :type name: str
        :returns: new _APINode with /<name> on the end
        :rtype: _APINode
        """
        # '-' can't be used in variable names
        if name in ('storage_pools', 'virtual_machines'):
            name = name.replace('_', '-')
        return self.__class__('{}/{}'.format(self._api_endpoint, name),
                              cert=self.session.cert,
                              verify=self.session.verify,
                              timeout=self._timeout)

    def __getitem__(self, item):
        """This converts python api.thing[name] -> ".../thing/name"

        :param item: the 'thing' in the square-braces in a python expr.
        :type item: str
        :returns: A new _APINode(with the new item tagged on as /<item>
        :rtype: _APINode
        """
        return self.__class__('{}/{}'.format(self._api_endpoint, item),
                              cert=self.session.cert,
                              verify=self.session.verify,
                              timeout=self._timeout)

    def _assert_response(self, response, allowed_status_codes=(200,),
                         stream=False, is_api=True):
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
                if data['type'] != 'sync':
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
        is_api = kwargs.pop('is_api', True)
        kwargs['timeout'] = kwargs.get('timeout', self._timeout)
        response = self.session.get(self._api_endpoint, *args, **kwargs)
        self._assert_response(response,
                              stream=kwargs.get('stream', False),
                              is_api=is_api)
        return response

    def post(self, *args, **kwargs):
        """Perform an HTTP POST."""
        kwargs['timeout'] = kwargs.get('timeout', self._timeout)
        target = kwargs.pop("target", None)

        if target is not None:
            params = kwargs.get("params", {})
            params["target"] = target
            kwargs["params"] = params

        response = self.session.post(self._api_endpoint, *args, **kwargs)
        # Prior to LXD 2.0.3, successful synchronous requests returned 200,
        # rather than 201.
        self._assert_response(response, allowed_status_codes=(200, 201, 202))
        return response

    def put(self, *args, **kwargs):
        """Perform an HTTP PUT."""
        kwargs['timeout'] = kwargs.get('timeout', self._timeout)
        response = self.session.put(self._api_endpoint, *args, **kwargs)
        self._assert_response(response, allowed_status_codes=(200, 202))
        return response

    def patch(self, *args, **kwargs):
        """Perform an HTTP PATCH."""
        kwargs['timeout'] = kwargs.get('timeout', self._timeout)
        response = self.session.patch(self._api_endpoint, *args, **kwargs)
        self._assert_response(response, allowed_status_codes=(200, 202))
        return response

    def delete(self, *args, **kwargs):
        """Perform an HTTP delete."""
        kwargs['timeout'] = kwargs.get('timeout', self._timeout)
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
        json_message = json.loads(message.data.decode('utf-8'))
        self.messages.append(json_message)


class Client(object):
    """Client class for LXD REST API.

    This client wraps all the functionality required to interact with
    LXD, and is meant to be the sole entry point.

    .. attribute:: instances

        Instance of :class:`Client.Instances
        <pylxd.client.Client.Instances>`:

    .. attribute:: containers

        Instance of :class:`Client.Containers
        <pylxd.client.Client.Containers>`:

    .. attribute:: virtual_machines

        Instance of :class:`Client.VirtualMachines
        <pylxd.client.Client.VirtualMachines>`:

    .. attribute:: images

        Instance of :class:`Client.Images <pylxd.client.Client.Images>`.

    .. attribute:: operations

        Instance of :class:`Client.Operations
        <pylxd.client.Client.Operations>`.

    .. attribute:: profiles

        Instance of :class:`Client.Profiles <pylxd.client.Client.Profiles>`.

    .. attribute:: api

        This attribute provides tree traversal syntax to LXD's REST API for
        lower-level interaction.

        Use the name of the url part as attribute or item of an api object to
        create another api object appended with the new url part name, ie:

            >>> api = Client().api
            # /
            >>> response = api.get()
            # Check status code and response
            >>> print response.status_code, response.json()
            # /instances/test/
            >>> print api.instances['test'].get().json()

    """

    def __init__(
            self, endpoint=None, version='1.0', cert=None, verify=True,
            timeout=None):
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

        """

        self.cert = cert
        if endpoint is not None:
            if endpoint.startswith('/') and os.path.isfile(endpoint):
                self.api = _APINode('http+unix://{}'.format(
                    parse.quote(endpoint, safe='')), timeout=timeout)
            else:
                # Extra trailing slashes cause LXD to 301
                endpoint = endpoint.rstrip('/')
                if cert is None and (
                        os.path.exists(DEFAULT_CERTS.cert) and
                        os.path.exists(DEFAULT_CERTS.key)):
                    cert = DEFAULT_CERTS
                self.api = _APINode(
                    endpoint, cert=cert, verify=verify, timeout=timeout)
        else:
            if 'LXD_DIR' in os.environ:
                path = os.path.join(os.environ.get('LXD_DIR'), 'unix.socket')
            elif os.path.exists('/var/snap/lxd/common/lxd/unix.socket'):
                path = '/var/snap/lxd/common/lxd/unix.socket'
            else:
                path = '/var/lib/lxd/unix.socket'
            endpoint = 'http+unix://{}'.format(parse.quote(path, safe=''))
            self.api = _APINode(endpoint, timeout=timeout)
        self.api = self.api[version]

        # Verify the connection is valid.
        try:
            response = self.api.get()
            if response.status_code != 200:
                raise exceptions.ClientConnectionFailed()
            self.host_info = response.json()['metadata']

        except (requests.exceptions.ConnectionError,
                requests.exceptions.InvalidURL) as e:
            raise exceptions.ClientConnectionFailed(str(e))

        self.cluster = managers.ClusterManager(self)
        self.certificates = managers.CertificateManager(self)
        self.instances = managers.InstanceManager(self)
        self.containers = managers.ContainerManager(self)
        self.virtual_machines = managers.VirtualMachineManager(self)
        self.images = managers.ImageManager(self)
        self.networks = managers.NetworkManager(self)
        self.operations = managers.OperationManager(self)
        self.profiles = managers.ProfileManager(self)
        self.storage_pools = managers.StoragePoolManager(self)
        self._resource_cache = None

    @property
    def trusted(self):
        return self.host_info['auth'] == 'trusted'

    @property
    def resources(self):
        if self._resource_cache is None:
            self.assert_has_api_extension('resources')
            response = self.api.resources.get()
            if response.status_code != 200:
                raise exceptions.ClientConnectionFailed()
            self._resource_cache = response.json()['metadata']
        return self._resource_cache

    def has_api_extension(self, name):
        """Return True if the `name` api extension exists.

        :param name: the api_extension to look for.
        :type name: str
        :returns: True if extension exists
        :rtype: bool
        """
        return name in self.host_info['api_extensions']

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

    def authenticate(self, password):
        if self.trusted:
            return
        cert = open(self.api.session.cert[0]).read().encode('utf-8')
        self.certificates.create(password, cert)

        # Refresh the host info
        response = self.api.get()
        self.host_info = response.json()['metadata']

    @property
    def websocket_url(self):
        if self.api.scheme in ('http', 'https'):
            host = self.api.netloc
            if self.api.scheme == 'http':
                scheme = 'ws'
            else:
                scheme = 'wss'
        else:
            scheme = 'ws+unix'
            host = parse.unquote(self.api.netloc)
        url = parse.urlunparse((scheme, host, '', '', '', ''))
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
        :rtype: Option[_WebsocketClient(), :param:`websocket_client`]
        """
        if not _ws4py_installed:
            raise ValueError(
                'This feature requires the optional ws4py library.')
        if websocket_client is None:
            websocket_client = _WebsocketClient

        client = websocket_client(self.websocket_url)
        parsed = parse.urlparse(self.api.events._api_endpoint)

        resource = parsed.path

        if event_types and EventType.All not in event_types:
            query = parse.parse_qs(parsed.query)
            query.update({'type': ','.join(t.value for t in event_types)})
            resource = '{}?{}'.format(resource, parse.urlencode(query))

        client.resource = resource

        return client
