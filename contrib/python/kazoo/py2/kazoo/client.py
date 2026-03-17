"""Kazoo Zookeeper Client"""
from collections import defaultdict, deque
from functools import partial
import inspect
import logging
from os.path import split
import re
import warnings

import six

from kazoo.exceptions import (
    AuthFailedError,
    ConfigurationError,
    ConnectionClosedError,
    ConnectionLoss,
    KazooException,
    NoNodeError,
    NodeExistsError,
    SessionExpiredError,
    WriterNotClosedException,
)
from kazoo.handlers.threading import SequentialThreadingHandler
from kazoo.handlers.utils import capture_exceptions, wrap
from kazoo.hosts import collect_hosts
from kazoo.loggingsupport import BLATHER
from kazoo.protocol.connection import ConnectionHandler
from kazoo.protocol.paths import _prefix_root, normpath
from kazoo.protocol.serialization import (
    Auth,
    CheckVersion,
    CloseInstance,
    Create,
    Create2,
    Delete,
    Exists,
    GetChildren,
    GetChildren2,
    GetACL,
    SetACL,
    GetData,
    Reconfig,
    SetData,
    Sync,
    Transaction
)
from kazoo.protocol.states import (
    Callback,
    EventType,
    KazooState,
    KeeperState,
    WatchedEvent
)
from kazoo.retry import KazooRetry
from kazoo.security import ACL, OPEN_ACL_UNSAFE

# convenience API
from kazoo.recipe.barrier import Barrier, DoubleBarrier
from kazoo.recipe.counter import Counter
from kazoo.recipe.election import Election
from kazoo.recipe.lease import NonBlockingLease, MultiNonBlockingLease
from kazoo.recipe.lock import Lock, ReadLock, WriteLock, Semaphore
from kazoo.recipe.partitioner import SetPartitioner
from kazoo.recipe.party import Party, ShallowParty
from kazoo.recipe.queue import Queue, LockingQueue
from kazoo.recipe.watchers import ChildrenWatch, DataWatch


string_types = six.string_types
bytes_types = (six.binary_type,)

CLOSED_STATES = (
    KeeperState.EXPIRED_SESSION,
    KeeperState.AUTH_FAILED,
    KeeperState.CLOSED
)
ENVI_VERSION = re.compile(r'([\d\.]*).*', re.DOTALL)
ENVI_VERSION_KEY = 'zookeeper.version'
log = logging.getLogger(__name__)


_RETRY_COMPAT_DEFAULTS = dict(
    max_retries=None,
    retry_delay=0.1,
    retry_backoff=2,
    retry_max_delay=3600,
)

_RETRY_COMPAT_MAPPING = dict(
    max_retries='max_tries',
    retry_delay='delay',
    retry_backoff='backoff',
    retry_max_delay='max_delay',
)


class KazooClient(object):
    """An Apache Zookeeper Python client supporting alternate callback
    handlers and high-level functionality.

    Watch functions registered with this class will not get session
    events, unlike the default Zookeeper watches. They will also be
    called with a single argument, a
    :class:`~kazoo.protocol.states.WatchedEvent` instance.

    """
    def __init__(self, hosts='127.0.0.1:2181',
                 timeout=10.0, client_id=None, handler=None,
                 default_acl=None, auth_data=None, sasl_options=None,
                 read_only=None, randomize_hosts=True, connection_retry=None,
                 command_retry=None, logger=None, keyfile=None,
                 keyfile_password=None, certfile=None, ca=None,
                 use_ssl=False, verify_certs=True, **kwargs):
        """Create a :class:`KazooClient` instance. All time arguments
        are in seconds.

        :param hosts: Comma-separated list of hosts to connect to
                      (e.g. 127.0.0.1:2181,127.0.0.1:2182,[::1]:2183).
        :param timeout: The longest to wait for a Zookeeper connection.
        :param client_id: A Zookeeper client id, used when
                          re-establishing a prior session connection.
        :param handler: An instance of a class implementing the
                        :class:`~kazoo.interfaces.IHandler` interface
                        for callback handling.
        :param default_acl: A default ACL used on node creation.
        :param auth_data:
            A list of authentication credentials to use for the
            connection. Should be a list of (scheme, credential)
            tuples as :meth:`add_auth` takes.
        :param sasl_options:
            SASL options for the connection, if SASL support is to be used.
            Should be a dict of SASL options passed to the underlying
            `pure-sasl <https://pypi.org/project/pure-sasl>`_ library.

            For example using the DIGEST-MD5 mechnism:

            .. code-block:: python

                sasl_options = {
                    'mechanism': 'DIGEST-MD5',
                    'username': 'myusername',
                    'password': 'mypassword'
                }

            For GSSAPI, using the running process' ticket cache:

            .. code-block:: python

                sasl_options = {
                    'mechanism': 'GSSAPI',
                    'service': 'myzk',                  # optional
                    'principal': 'client@EXAMPLE.COM'   # optional
                }

        :param read_only: Allow connections to read only servers.
        :param randomize_hosts: By default randomize host selection.
        :param connection_retry:
            A :class:`kazoo.retry.KazooRetry` object to use for
            retrying the connection to Zookeeper. Also can be a dict of
            options which will be used for creating one.
        :param command_retry:
            A :class:`kazoo.retry.KazooRetry` object to use for
            the :meth:`KazooClient.retry` method. Also can be a dict of
            options which will be used for creating one.
        :param logger: A custom logger to use instead of the module
            global `log` instance.
        :param keyfile: SSL keyfile to use for authentication
        :param keyfile_password: SSL keyfile password
        :param certfile: SSL certfile to use for authentication
        :param ca: SSL CA file to use for authentication
        :param use_ssl: argument to control whether SSL is used or not
        :param verify_certs: when using SSL, argument to bypass
            certs verification

        Basic Example:

        .. code-block:: python

            zk = KazooClient()
            zk.start()
            children = zk.get_children('/')
            zk.stop()

        As a convenience all recipe classes are available as attributes
        and get automatically bound to the client. For example::

            zk = KazooClient()
            zk.start()
            lock = zk.Lock('/lock_path')

        .. versionadded:: 0.6
            The read_only option. Requires Zookeeper 3.4+

        .. versionadded:: 0.6
            The retry_max_delay option.

        .. versionadded:: 0.6
            The randomize_hosts option.

        .. versionchanged:: 0.8
            Removed the unused watcher argument (was second argument).

        .. versionadded:: 1.2
            The connection_retry, command_retry and logger options.

        .. versionadded:: 2.7
            The sasl_options option.

        """
        self.logger = logger or log

        # Record the handler strategy used
        self.handler = handler if handler else SequentialThreadingHandler()
        if inspect.isclass(self.handler):
            raise ConfigurationError("Handler must be an instance of a class, "
                                     "not the class: %s" % self.handler)

        self.auth_data = auth_data if auth_data else set([])
        self.default_acl = default_acl
        self.randomize_hosts = randomize_hosts
        self.hosts = None
        self.chroot = None
        self.set_hosts(hosts)

        self.use_ssl = use_ssl
        self.verify_certs = verify_certs
        self.certfile = certfile
        self.keyfile = keyfile
        self.keyfile_password = keyfile_password
        self.ca = ca
        # Curator like simplified state tracking, and listeners for
        # state transitions
        self._state = KeeperState.CLOSED
        self.state = KazooState.LOST
        self.state_listeners = set()
        self._child_watchers = defaultdict(set)
        self._data_watchers = defaultdict(set)
        self._reset()
        self.read_only = read_only

        if client_id:
            self._session_id = client_id[0]
            self._session_passwd = client_id[1]
        else:
            self._reset_session()

        # ZK uses milliseconds
        self._session_timeout = int(timeout * 1000)

        # We use events like twitter's client to track current and
        # desired state (connected, and whether to shutdown)
        self._live = self.handler.event_object()
        self._writer_stopped = self.handler.event_object()
        self._stopped = self.handler.event_object()
        self._stopped.set()
        self._writer_stopped.set()

        self.retry = self._conn_retry = None

        if type(connection_retry) is dict:
            self._conn_retry = KazooRetry(**connection_retry)
        elif type(connection_retry) is KazooRetry:
            self._conn_retry = connection_retry

        if type(command_retry) is dict:
            self.retry = KazooRetry(**command_retry)
        elif type(command_retry) is KazooRetry:
            self.retry = command_retry

        if type(self._conn_retry) is KazooRetry:
            if self.handler.sleep_func != self._conn_retry.sleep_func:
                raise ConfigurationError("Retry handler and event handler "
                                         " must use the same sleep func")

        if type(self.retry) is KazooRetry:
            if self.handler.sleep_func != self.retry.sleep_func:
                raise ConfigurationError(
                    "Command retry handler and event handler "
                    "must use the same sleep func")

        if self.retry is None or self._conn_retry is None:
            old_retry_keys = dict(_RETRY_COMPAT_DEFAULTS)
            for key in old_retry_keys:
                try:
                    old_retry_keys[key] = kwargs.pop(key)
                    warnings.warn(
                        'Passing retry configuration param %s to the '
                        'client directly is deprecated, please pass a '
                        'configured retry object (using param %s)' % (
                            key, _RETRY_COMPAT_MAPPING[key]),
                        DeprecationWarning, stacklevel=2)
                except KeyError:
                    pass

            retry_keys = {}
            for oldname, value in old_retry_keys.items():
                retry_keys[_RETRY_COMPAT_MAPPING[oldname]] = value

            if self._conn_retry is None:
                self._conn_retry = KazooRetry(
                    sleep_func=self.handler.sleep_func,
                    **retry_keys)
            if self.retry is None:
                self.retry = KazooRetry(
                    sleep_func=self.handler.sleep_func,
                    **retry_keys)

        # Managing legacy SASL options
        for scheme, auth in self.auth_data:
            if scheme != 'sasl':
                continue
            if sasl_options:
                raise ConfigurationError(
                    'Multiple SASL configurations provided'
                )
            warnings.warn(
                'Passing SASL configuration as part of the auth_data is '
                'deprecated, please use the sasl_options configuration '
                'instead', DeprecationWarning, stacklevel=2
            )
            username, password = auth.split(':')
            # Generate an equivalent SASL configuration
            sasl_options = {
                'username': username,
                'password': password,
                'mechanism': 'DIGEST-MD5',
                'service': 'zookeeper',
                'principal': 'zk-sasl-md5',
            }
        # Cleanup
        self.auth_data = set([
            (scheme, auth)
            for scheme, auth in self.auth_data
            if scheme != 'sasl'
        ])

        self._conn_retry.interrupt = lambda: self._stopped.is_set()
        self._connection = ConnectionHandler(
            self, self._conn_retry.copy(), logger=self.logger,
            sasl_options=sasl_options)

        # Every retry call should have its own copy of the retry helper
        # to avoid shared retry counts
        self._retry = self.retry

        def _retry(*args, **kwargs):
            return self._retry.copy()(*args, **kwargs)
        self.retry = _retry

        self.Barrier = partial(Barrier, self)
        self.Counter = partial(Counter, self)
        self.DoubleBarrier = partial(DoubleBarrier, self)
        self.ChildrenWatch = partial(ChildrenWatch, self)
        self.DataWatch = partial(DataWatch, self)
        self.Election = partial(Election, self)
        self.NonBlockingLease = partial(NonBlockingLease, self)
        self.MultiNonBlockingLease = partial(MultiNonBlockingLease, self)
        self.Lock = partial(Lock, self)
        self.ReadLock = partial(ReadLock, self)
        self.WriteLock = partial(WriteLock, self)
        self.Party = partial(Party, self)
        self.Queue = partial(Queue, self)
        self.LockingQueue = partial(LockingQueue, self)
        self.SetPartitioner = partial(SetPartitioner, self)
        self.Semaphore = partial(Semaphore, self)
        self.ShallowParty = partial(ShallowParty, self)

        # If we got any unhandled keywords, complain like Python would
        if kwargs:
            raise TypeError('__init__() got unexpected keyword arguments: %s'
                            % (kwargs.keys(),))

    def _reset(self):
        """Resets a variety of client states for a new connection."""
        self._queue = deque()
        self._pending = deque()

        self._reset_watchers()
        self._reset_session()
        self.last_zxid = 0
        self._protocol_version = None

    def _reset_watchers(self):
        watchers = []
        for child_watchers in six.itervalues(self._child_watchers):
            watchers.extend(child_watchers)

        for data_watchers in six.itervalues(self._data_watchers):
            watchers.extend(data_watchers)

        self._child_watchers = defaultdict(set)
        self._data_watchers = defaultdict(set)

        ev = WatchedEvent(EventType.NONE, self._state, None)
        for watch in watchers:
            self.handler.dispatch_callback(Callback("watch", watch, (ev,)))

    def _reset_session(self):
        self._session_id = None
        self._session_passwd = b'\x00' * 16

    @property
    def client_state(self):
        """Returns the last Zookeeper client state

        This is the non-simplified state information and is generally
        not as useful as the simplified KazooState information.

        """
        return self._state

    @property
    def client_id(self):
        """Returns the client id for this Zookeeper session if
        connected.

        :returns: client id which consists of the session id and
                  password.
        :rtype: tuple
        """
        if self._live.is_set():
            return (self._session_id, self._session_passwd)
        return None

    @property
    def connected(self):
        """Returns whether the Zookeeper connection has been
        established."""
        return self._live.is_set()

    def set_hosts(self, hosts, randomize_hosts=None):
        """ sets the list of hosts used by this client.

        This function accepts the same format hosts parameter as the init
        function and sets the client to use the new hosts the next time it
        needs to look up a set of hosts. This function does not affect the
        current connected status.

        It is not currently possible to change the chroot with this function,
        setting a host list with a new chroot will raise a ConfigurationError.

        :param hosts: see description in :meth:`KazooClient.__init__`
        :param randomize_hosts: override client default for host randomization
        :raises:
            :exc:`ConfigurationError` if the hosts argument changes the chroot

        .. versionadded:: 1.4

        .. warning::

            Using this function to point a client to a completely disparate
            zookeeper server cluster has undefined behavior.

        """

        # Change the client setting for randomization if specified
        if randomize_hosts is not None:
            self.randomize_hosts = randomize_hosts

        # Randomizing the list will be done at connect time
        self.hosts, chroot = collect_hosts(hosts)

        if chroot:
            new_chroot = normpath(chroot)
        else:
            new_chroot = ''

        if self.chroot is not None and new_chroot != self.chroot:
            raise ConfigurationError("Changing chroot at runtime is not "
                                     "currently supported")

        self.chroot = new_chroot

    def add_listener(self, listener):
        """Add a function to be called for connection state changes.

        This function will be called with a
        :class:`~kazoo.protocol.states.KazooState` instance indicating
        the new connection state on state transitions.

        .. warning::

            This function must not block. If its at all likely that it
            might need data or a value that could result in blocking
            than the :meth:`~kazoo.interfaces.IHandler.spawn` method
            should be used so that the listener can return immediately.

        """
        if not (listener and callable(listener)):
            raise ConfigurationError("listener must be callable")
        self.state_listeners.add(listener)

    def remove_listener(self, listener):
        """Remove a listener function"""
        self.state_listeners.discard(listener)

    def _make_state_change(self, state):
        # skip if state is current
        if self.state == state:
            return

        self.state = state

        # Create copy of listeners for iteration in case one needs to
        # remove itself
        for listener in list(self.state_listeners):
            try:
                remove = listener(state)
                if remove is True:
                    self.remove_listener(listener)
            except Exception:
                self.logger.exception("Error in connection state listener")

    def _session_callback(self, state):
        if state == self._state:
            return

        # Note that we don't check self.state == LOST since that's also
        # the client's initial state
        closed_state = self._state in CLOSED_STATES
        self._state = state

        # If we were previously closed or had an expired session, and
        # are now connecting, don't bother with the rest of the
        # transitions since they only apply after
        # we've established a connection
        if closed_state and state == KeeperState.CONNECTING:
            self.logger.log(BLATHER, "Skipping state change")
            return

        if state in (KeeperState.CONNECTED, KeeperState.CONNECTED_RO):
            self.logger.info("Zookeeper connection established, "
                             "state: %s", state)
            self._live.set()
            self._make_state_change(KazooState.CONNECTED)
        elif state in CLOSED_STATES:
            self.logger.info("Zookeeper session closed, state: %s", state)
            self._live.clear()
            self._make_state_change(KazooState.LOST)
            self._notify_pending(state)
            self._reset()
        else:
            self.logger.info("Zookeeper connection lost")
            # Connection lost
            self._live.clear()
            self._notify_pending(state)
            self._make_state_change(KazooState.SUSPENDED)
            self._reset_watchers()

    def _notify_pending(self, state):
        """Used to clear a pending response queue and request queue
        during connection drops."""
        if state == KeeperState.AUTH_FAILED:
            exc = AuthFailedError()
        elif state == KeeperState.EXPIRED_SESSION:
            exc = SessionExpiredError()
        else:
            exc = ConnectionLoss()

        while True:
            try:
                request, async_object, xid = self._pending.popleft()
                if async_object:
                    async_object.set_exception(exc)
            except IndexError:
                break

        while True:
            try:
                request, async_object = self._queue.popleft()
                if async_object:
                    async_object.set_exception(exc)
            except IndexError:
                break

    def _safe_close(self):
        self.handler.stop()
        timeout = self._session_timeout // 1000
        if timeout < 10:
            timeout = 10
        if not self._connection.stop(timeout):
            raise WriterNotClosedException(
                "Writer still open from prior connection "
                "and wouldn't close after %s seconds" % timeout)

    def _call(self, request, async_object):
        """Ensure the client is in CONNECTED or SUSPENDED state and put the
        request in the queue if it is.

        Returns False if the call short circuits due to AUTH_FAILED,
        CLOSED, or EXPIRED_SESSION state.

        """

        if self._state == KeeperState.AUTH_FAILED:
            async_object.set_exception(AuthFailedError())
            return False
        elif self._state == KeeperState.CLOSED:
            async_object.set_exception(ConnectionClosedError(
                "Connection has been closed"))
            return False
        elif self._state == KeeperState.EXPIRED_SESSION:
            async_object.set_exception(SessionExpiredError())
            return False

        self._queue.append((request, async_object))

        # wake the connection, guarding against a race with close()
        write_sock = self._connection._write_sock
        if write_sock is None:
            async_object.set_exception(ConnectionClosedError(
                "Connection has been closed"))
        try:
            write_sock.send(b'\0')
        except:  # NOQA
            async_object.set_exception(ConnectionClosedError(
                "Connection has been closed"))

    def start(self, timeout=15):
        """Initiate connection to ZK.

        :param timeout: Time in seconds to wait for connection to
                        succeed.
        :raises: :attr:`~kazoo.interfaces.IHandler.timeout_exception`
                 if the connection wasn't established within `timeout`
                 seconds.

        """
        event = self.start_async()
        event.wait(timeout=timeout)
        if not self.connected:
            # We time-out, ensure we are disconnected
            self.stop()
            self.close()
            raise self.handler.timeout_exception("Connection time-out")

        if self.chroot and not self.exists("/"):
            warnings.warn("No chroot path exists, the chroot path "
                          "should be created before normal use.")

    def start_async(self):
        """Asynchronously initiate connection to ZK.

        :returns: An event object that can be checked to see if the
                  connection is alive.
        :rtype: :class:`~threading.Event` compatible object.

        """
        # If we're already connected, ignore
        if self._live.is_set():
            return self._live

        # Make sure we're safely closed
        self._safe_close()

        # We've been asked to connect, clear the stop and our writer
        # thread indicator
        self._stopped.clear()
        self._writer_stopped.clear()

        # Start the handler
        self.handler.start()

        # Start the connection
        self._connection.start()
        return self._live

    def stop(self):
        """Gracefully stop this Zookeeper session.

        This method can be called while a reconnection attempt is in
        progress, which will then be halted.

        Once the connection is closed, its session becomes invalid. All
        the ephemeral nodes in the ZooKeeper server associated with the
        session will be removed. The watches left on those nodes (and
        on their parents) will be triggered.

        """
        if self._stopped.is_set():
            return

        self._stopped.set()
        self._queue.append((CloseInstance, None))
        try:
            self._connection._write_sock.send(b'\0')
        finally:
            self._safe_close()

    def restart(self):
        """Stop and restart the Zookeeper session."""
        self.stop()
        self.start()

    def close(self):
        """Free any resources held by the client.

        This method should be called on a stopped client before it is
        discarded. Not doing so may result in filehandles being leaked.

        .. versionadded:: 1.0
        """
        self._connection.close()

    def command(self, cmd=b'ruok'):
        """Sent a management command to the current ZK server.

        Examples are `ruok`, `envi` or `stat`.

        :returns: An unstructured textual response.
        :rtype: str

        :raises:
            :exc:`ConnectionLoss` if there is no connection open, or
            possibly a :exc:`socket.error` if there's a problem with
            the connection used just for this command.

        .. versionadded:: 0.5

        """

        def read_all(sock):
            while True:
                part = sock.recv(8192)
                if len(part) == 0:
                    break
                else:
                    yield part

        if not self._live.is_set():
            raise ConnectionLoss("No connection to server")

        peer = self._connection._socket.getpeername()[:2]
        sock = self.handler.create_connection(
            peer, timeout=self._session_timeout / 1000.0,
            use_ssl=self.use_ssl,
            ca=self.ca,
            certfile=self.certfile,
            keyfile=self.keyfile,
            keyfile_password=self.keyfile_password,
            verify_certs=self.verify_certs,
        )
        sock.sendall(cmd)
        result = ''.join(read_all(sock))
        sock.close()
        return result.decode('utf-8', 'replace')

    def server_version(self, retries=3):
        """Get the version of the currently connected ZK server.

        :returns: The server version, for example (3, 4, 3).
        :rtype: tuple

        .. versionadded:: 0.5

        """
        def _try_fetch():
            data = self.command(b'envi')
            data_parsed = {}
            for line in data.splitlines():
                try:
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                except ValueError:
                    pass
                else:
                    if k:
                        data_parsed[k] = v
            version = data_parsed.get(ENVI_VERSION_KEY, '')
            version_digits = ENVI_VERSION.match(version).group(1)
            try:
                return tuple([int(d) for d in version_digits.split('.')])
            except ValueError:
                return None

        def _is_valid(version):
            # All zookeeper versions should have at least major.minor
            # version numbers; if we get one that doesn't it is likely not
            # correct and was truncated...
            if version and len(version) > 1:
                return True
            return False

        # Try 1 + retries amount of times to get a version that we know
        # will likely be acceptable...
        version = _try_fetch()
        if _is_valid(version):
            return version
        for _i in six.moves.range(0, retries):
            version = _try_fetch()
            if _is_valid(version):
                return version
        raise KazooException("Unable to fetch useable server"
                             " version after trying %s times"
                             % (1 + max(0, retries)))

    def add_auth(self, scheme, credential):
        """Send credentials to server.

        :param scheme: authentication scheme (default supported:
                       "digest").
        :param credential: the credential -- value depends on scheme.

        :returns: True if it was successful.
        :rtype: bool

        :raises:
            :exc:`~kazoo.exceptions.AuthFailedError` if it failed though
            the session state will be set to AUTH_FAILED as well.

        """
        return self.add_auth_async(scheme, credential).get()

    def add_auth_async(self, scheme, credential):
        """Asynchronously send credentials to server. Takes the same
        arguments as :meth:`add_auth`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(scheme, string_types):
            raise TypeError("Invalid type for 'scheme' (string expected)")
        if not isinstance(credential, string_types):
            raise TypeError("Invalid type for 'credential' (string expected)")

        # we need this auth data to re-authenticate on reconnect
        self.auth_data.add((scheme, credential))

        async_result = self.handler.async_result()
        self._call(Auth(0, scheme, credential), async_result)
        return async_result

    def unchroot(self, path):
        """Strip the chroot if applicable from the path."""
        if not self.chroot:
            return path
        if self.chroot == path:
            return "/"
        if path.startswith(self.chroot):
            return path[len(self.chroot):]
        else:
            return path

    def sync_async(self, path):
        """Asynchronous sync.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        async_result = self.handler.async_result()

        @wrap(async_result)
        def _sync_completion(result):
            return self.unchroot(result.get())

        def _do_sync():
            result = self.handler.async_result()
            self._call(
                Sync(_prefix_root(self.chroot, path)),
                result
            )
            result.rawlink(_sync_completion)

        _do_sync()
        return async_result

    def sync(self, path):
        """Sync, blocks until response is acknowledged.

        Flushes channel between process and leader.

        :param path: path of node.
        :returns: The node path that was synced.
        :raises:
            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        .. versionadded:: 0.5

        """
        return self.sync_async(path).get()

    def create(self, path, value=b"", acl=None, ephemeral=False,
               sequence=False, makepath=False, include_data=False):
        """Create a node with the given value as its data. Optionally
        set an ACL on the node.

        The ephemeral and sequence arguments determine the type of the
        node.

        An ephemeral node will be automatically removed by ZooKeeper
        when the session associated with the creation of the node
        expires.

        A sequential node will be given the specified path plus a
        suffix `i` where i is the current sequential number of the
        node. The sequence number is always fixed length of 10 digits,
        0 padded. Once such a node is created, the sequential number
        will be incremented by one.

        If a node with the same actual path already exists in
        ZooKeeper, a NodeExistsError will be raised. Note that since a
        different actual path is used for each invocation of creating
        sequential nodes with the same path argument, the call will
        never raise NodeExistsError.

        If the parent node does not exist in ZooKeeper, a NoNodeError
        will be raised. Setting the optional `makepath` argument to
        `True` will create all missing parent nodes instead.

        An ephemeral node cannot have children. If the parent node of
        the given path is ephemeral, a NoChildrenForEphemeralsError
        will be raised.

        This operation, if successful, will trigger all the watches
        left on the node of the given path by :meth:`exists` and
        :meth:`get` API calls, and the watches left on the parent node
        by :meth:`get_children` API calls.

        The maximum allowable size of the node value is 1 MB. Values
        larger than this will cause a ZookeeperError to be raised.

        :param path: Path of node.
        :param value: Initial bytes value of node.
        :param acl: :class:`~kazoo.security.ACL` list.
        :param ephemeral: Boolean indicating whether node is ephemeral
                          (tied to this session).
        :param sequence: Boolean indicating whether path is suffixed
                         with a unique index.
        :param makepath: Whether the path should be created if it
                         doesn't exist.
        :param include_data:
            Include the :class:`~kazoo.protocol.states.ZnodeStat` of
            the node in addition to its real path. This option changes
            the return value to be a tuple of (path, stat).

        :returns: Real path of the new node, or tuple if `include_data`
                  is `True`
        :rtype: str

        :raises:
            :exc:`~kazoo.exceptions.NodeExistsError` if the node
            already exists.

            :exc:`~kazoo.exceptions.NoNodeError` if parent nodes are
            missing.

            :exc:`~kazoo.exceptions.NoChildrenForEphemeralsError` if
            the parent node is an ephemeral node.

            :exc:`~kazoo.exceptions.ZookeeperError` if the provided
            value is too large.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        .. versionadded:: 1.1
            The `makepath` option.
        .. versionadded:: 2.7
            The `include_data` option.
        """
        acl = acl or self.default_acl
        return self.create_async(
            path, value, acl=acl, ephemeral=ephemeral,
            sequence=sequence, makepath=makepath, include_data=include_data
        ).get()

    def create_async(self, path, value=b"", acl=None, ephemeral=False,
                     sequence=False, makepath=False, include_data=False):
        """Asynchronously create a ZNode. Takes the same arguments as
        :meth:`create`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        .. versionadded:: 1.1
            The makepath option.
        .. versionadded:: 2.7
            The `include_data` option.
        """
        if acl is None and self.default_acl:
            acl = self.default_acl

        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if acl and (isinstance(acl, ACL) or
                    not isinstance(acl, (tuple, list))):
            raise TypeError("Invalid type for 'acl' (acl must be a tuple/list"
                            " of ACL's")
        if value is not None and not isinstance(value, bytes_types):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if not isinstance(ephemeral, bool):
            raise TypeError("Invalid type for 'ephemeral' (bool expected)")
        if not isinstance(sequence, bool):
            raise TypeError("Invalid type for 'sequence' (bool expected)")
        if not isinstance(makepath, bool):
            raise TypeError("Invalid type for 'makepath' (bool expected)")
        if not isinstance(include_data, bool):
            raise TypeError("Invalid type for 'include_data' (bool expected)")

        flags = 0
        if ephemeral:
            flags |= 1
        if sequence:
            flags |= 2
        if acl is None:
            acl = OPEN_ACL_UNSAFE

        async_result = self.handler.async_result()

        @capture_exceptions(async_result)
        def do_create():
            result = self._create_async_inner(
                path, value, acl, flags,
                trailing=sequence, include_data=include_data
            )
            result.rawlink(create_completion)

        @capture_exceptions(async_result)
        def retry_completion(result):
            result.get()
            do_create()

        @wrap(async_result)
        def create_completion(result):
            try:
                if include_data:
                    new_path, stat = result.get()
                    return self.unchroot(new_path), stat
                else:
                    return self.unchroot(result.get())
            except NoNodeError:
                if not makepath:
                    raise
                if sequence and path.endswith('/'):
                    parent = path.rstrip('/')
                else:
                    parent, _ = split(path)
                self.ensure_path_async(parent, acl).rawlink(retry_completion)

        do_create()
        return async_result

    def _create_async_inner(self, path, value, acl, flags,
                            trailing=False, include_data=False):
        async_result = self.handler.async_result()
        if include_data:
            opcode = Create2
        else:
            opcode = Create

        call_result = self._call(
            opcode(_prefix_root(self.chroot, path, trailing=trailing),
                   value, acl, flags), async_result)
        if call_result is False:
            # We hit a short-circuit exit on the _call. Because we are
            # not using the original async_result here, we bubble the
            # exception upwards to the do_create function in
            # KazooClient.create so that it gets set on the correct
            # async_result object
            raise async_result.exception
        return async_result

    def ensure_path(self, path, acl=None):
        """Recursively create a path if it doesn't exist.

        :param path: Path of node.
        :param acl: Permissions for node.

        """
        return self.ensure_path_async(path, acl).get()

    def ensure_path_async(self, path, acl=None):
        """Recursively create a path asynchronously if it doesn't
        exist. Takes the same arguments as :meth:`ensure_path`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        .. versionadded:: 1.1

        """
        acl = acl or self.default_acl
        async_result = self.handler.async_result()

        @wrap(async_result)
        def create_completion(result):
            try:
                return result.get()
            except NodeExistsError:
                return True

        @capture_exceptions(async_result)
        def prepare_completion(next_path, result):
            result.get()
            self.create_async(next_path, acl=acl).rawlink(create_completion)

        @wrap(async_result)
        def exists_completion(path, result):
            if result.get():
                return True
            parent, node = split(path)
            if node:
                self.ensure_path_async(parent, acl=acl).rawlink(
                    partial(prepare_completion, path))
            else:
                self.create_async(path, acl=acl).rawlink(create_completion)

        self.exists_async(path).rawlink(partial(exists_completion, path))

        return async_result

    def exists(self, path, watch=None):
        """Check if a node exists.

        If a watch is provided, it will be left on the node with the
        given path. The watch will be triggered by a successful
        operation that creates/deletes the node or sets the data on the
        node.

        :param path: Path of node.
        :param watch: Optional watch callback to set for future changes
                      to this path.
        :returns: ZnodeStat of the node if it exists, else None if the
                  node does not exist.
        :rtype: :class:`~kazoo.protocol.states.ZnodeStat` or `None`.

        :raises:
            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        """
        return self.exists_async(path, watch=watch).get()

    def exists_async(self, path, watch=None):
        """Asynchronously check if a node exists. Takes the same
        arguments as :meth:`exists`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if watch and not callable(watch):
            raise TypeError("Invalid type for 'watch' (must be a callable)")

        async_result = self.handler.async_result()
        self._call(Exists(_prefix_root(self.chroot, path), watch),
                   async_result)
        return async_result

    def get(self, path, watch=None):
        """Get the value of a node.

        If a watch is provided, it will be left on the node with the
        given path. The watch will be triggered by a successful
        operation that sets data on the node, or deletes the node.

        :param path: Path of node.
        :param watch: Optional watch callback to set for future changes
                      to this path.
        :returns:
            Tuple (value, :class:`~kazoo.protocol.states.ZnodeStat`) of
            node.
        :rtype: tuple

        :raises:
            :exc:`~kazoo.exceptions.NoNodeError` if the node doesn't
            exist

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code

        """
        return self.get_async(path, watch=watch).get()

    def get_async(self, path, watch=None):
        """Asynchronously get the value of a node. Takes the same
        arguments as :meth:`get`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if watch and not callable(watch):
            raise TypeError("Invalid type for 'watch' (must be a callable)")

        async_result = self.handler.async_result()
        self._call(GetData(_prefix_root(self.chroot, path), watch),
                   async_result)
        return async_result

    def get_children(self, path, watch=None, include_data=False):
        """Get a list of child nodes of a path.

        If a watch is provided it will be left on the node with the
        given path. The watch will be triggered by a successful
        operation that deletes the node of the given path or
        creates/deletes a child under the node.

        The list of children returned is not sorted and no guarantee is
        provided as to its natural or lexical order.

        :param path: Path of node to list.
        :param watch: Optional watch callback to set for future changes
                      to this path.
        :param include_data:
            Include the :class:`~kazoo.protocol.states.ZnodeStat` of
            the node in addition to the children. This option changes
            the return value to be a tuple of (children, stat).

        :returns: List of child node names, or tuple if `include_data`
                  is `True`.
        :rtype: list

        :raises:
            :exc:`~kazoo.exceptions.NoNodeError` if the node doesn't
            exist.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        .. versionadded:: 0.5
            The `include_data` option.

        """
        return self.get_children_async(path, watch=watch,
                                       include_data=include_data).get()

    def get_children_async(self, path, watch=None, include_data=False):
        """Asynchronously get a list of child nodes of a path. Takes
        the same arguments as :meth:`get_children`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if watch and not callable(watch):
            raise TypeError("Invalid type for 'watch' (must be a callable)")
        if not isinstance(include_data, bool):
            raise TypeError("Invalid type for 'include_data' (bool expected)")

        async_result = self.handler.async_result()
        if include_data:
            req = GetChildren2(_prefix_root(self.chroot, path), watch)
        else:
            req = GetChildren(_prefix_root(self.chroot, path), watch)
        self._call(req, async_result)
        return async_result

    def get_acls(self, path):
        """Return the ACL and stat of the node of the given path.

        :param path: Path of the node.
        :returns: The ACL array of the given node and its
            :class:`~kazoo.protocol.states.ZnodeStat`.
        :rtype: tuple of (:class:`~kazoo.security.ACL` list,
                :class:`~kazoo.protocol.states.ZnodeStat`)
        :raises:
            :exc:`~kazoo.exceptions.NoNodeError` if the node doesn't
            exist.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code

        .. versionadded:: 0.5

        """
        return self.get_acls_async(path).get()

    def get_acls_async(self, path):
        """Return the ACL and stat of the node of the given path. Takes
        the same arguments as :meth:`get_acls`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")

        async_result = self.handler.async_result()
        self._call(GetACL(_prefix_root(self.chroot, path)), async_result)
        return async_result

    def set_acls(self, path, acls, version=-1):
        """Set the ACL for the node of the given path.

        Set the ACL for the node of the given path if such a node
        exists and the given version matches the version of the node.

        :param path: Path for the node.
        :param acls: List of :class:`~kazoo.security.ACL` objects to
                     set.
        :param version: The expected node version that must match.
        :returns: The stat of the node.
        :raises:
            :exc:`~kazoo.exceptions.BadVersionError` if version doesn't
            match.

            :exc:`~kazoo.exceptions.NoNodeError` if the node doesn't
            exist.

            :exc:`~kazoo.exceptions.InvalidACLError` if the ACL is
            invalid.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        .. versionadded:: 0.5

        """
        return self.set_acls_async(path, acls, version).get()

    def set_acls_async(self, path, acls, version=-1):
        """Set the ACL for the node of the given path. Takes the same
        arguments as :meth:`set_acls`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if isinstance(acls, ACL) or not isinstance(acls, (tuple, list)):
            raise TypeError("Invalid type for 'acl' (acl must be a tuple/list"
                            " of ACL's)")
        if not isinstance(version, int):
            raise TypeError("Invalid type for 'version' (int expected)")

        async_result = self.handler.async_result()
        self._call(SetACL(_prefix_root(self.chroot, path), acls, version),
                   async_result)
        return async_result

    def set(self, path, value, version=-1):
        """Set the value of a node.

        If the version of the node being updated is newer than the
        supplied version (and the supplied version is not -1), a
        BadVersionError will be raised.

        This operation, if successful, will trigger all the watches on
        the node of the given path left by :meth:`get` API calls.

        The maximum allowable size of the value is 1 MB. Values larger
        than this will cause a ZookeeperError to be raised.

        :param path: Path of node.
        :param value: New data value.
        :param version: Version of node being updated, or -1.
        :returns: Updated :class:`~kazoo.protocol.states.ZnodeStat` of
                  the node.

        :raises:
            :exc:`~kazoo.exceptions.BadVersionError` if version doesn't
            match.

            :exc:`~kazoo.exceptions.NoNodeError` if the node doesn't
            exist.

            :exc:`~kazoo.exceptions.ZookeeperError` if the provided
            value is too large.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        """
        return self.set_async(path, value, version).get()

    def set_async(self, path, value, version=-1):
        """Set the value of a node. Takes the same arguments as
        :meth:`set`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if value is not None and not isinstance(value, bytes_types):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if not isinstance(version, int):
            raise TypeError("Invalid type for 'version' (int expected)")

        async_result = self.handler.async_result()
        self._call(SetData(_prefix_root(self.chroot, path), value, version),
                   async_result)
        return async_result

    def transaction(self):
        """Create and return a :class:`TransactionRequest` object

        Creates a :class:`TransactionRequest` object. A Transaction can
        consist of multiple operations which can be committed as a
        single atomic unit. Either all of the operations will succeed
        or none of them.

        :returns: A TransactionRequest.
        :rtype: :class:`TransactionRequest`

        .. versionadded:: 0.6
            Requires Zookeeper 3.4+

        """
        return TransactionRequest(self)

    def delete(self, path, version=-1, recursive=False):
        """Delete a node.

        The call will succeed if such a node exists, and the given
        version matches the node's version (if the given version is -1,
        the default, it matches any node's versions).

        This operation, if successful, will trigger all the watches on
        the node of the given path left by `exists` API calls, and the
        watches on the parent node left by `get_children` API calls.

        :param path: Path of node to delete.
        :param version: Version of node to delete, or -1 for any.
        :param recursive: Recursively delete node and all its children,
                          defaults to False.
        :type recursive: bool

        :raises:
            :exc:`~kazoo.exceptions.BadVersionError` if version doesn't
            match.

            :exc:`~kazoo.exceptions.NoNodeError` if the node doesn't
            exist.

            :exc:`~kazoo.exceptions.NotEmptyError` if the node has
            children.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        """
        if not isinstance(recursive, bool):
            raise TypeError("Invalid type for 'recursive' (bool expected)")
        if recursive:
            return self._delete_recursive(path)
        else:
            return self.delete_async(path, version).get()

    def delete_async(self, path, version=-1):
        """Asynchronously delete a node. Takes the same arguments as
        :meth:`delete`, with the exception of `recursive`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(version, int):
            raise TypeError("Invalid type for 'version' (int expected)")
        async_result = self.handler.async_result()
        self._call(Delete(_prefix_root(self.chroot, path), version),
                   async_result)
        return async_result

    def _delete_recursive(self, path):
        try:
            children = self.get_children(path)
        except NoNodeError:
            return True

        if children:
            for child in children:
                if path == "/":
                    child_path = path + child
                else:
                    child_path = path + "/" + child

                self._delete_recursive(child_path)
        try:
            self.delete(path)
        except NoNodeError:  # pragma: nocover
            pass

    def reconfig(self, joining, leaving, new_members, from_config=-1):
        """Reconfig a cluster.

        This call will succeed if the cluster was reconfigured accordingly.

        :param joining: a comma separated list of servers being added
                        (see example for format) (incremental reconfiguration)
        :param leaving: a comma separated list of servers being removed
                        (see example for format) (incremental reconfiguration)
        :param new_members: a comma separated list of new membership
                            (non-incremental reconfiguration)
        :param from_config: version of the current configuration (optional -
                           causes reconfiguration to throw an exception if
                           configuration is no longer current)
        :type from_config: int
        :returns:
            Tuple (value, :class:`~kazoo.protocol.states.ZnodeStat`) of
            node.
        :rtype: tuple

        Basic Example:

        .. code-block:: python

            zk = KazooClient()
            zk.start()

            # first add an observer (incremental reconfiguration)
            joining = 'server.100=10.0.0.10:2889:3888:observer;0.0.0.0:2181'
            data, _ = zk.reconfig(
              joining=joining, leaving=None, new_members=None)

            # wait and then remove it (just by using its id) (incremental)
            data, _ = zk.reconfig(joining=None, leaving='100',
                                  new_members=None)

            # now do a full change of the cluster (non-incremental)
            new = [
              'server.100=10.0.0.10:2889:3888:observer;0.0.0.0:2181',
              'server.100=10.0.0.11:2889:3888:observer;0.0.0.0:2181',
              'server.100=10.0.0.12:2889:3888:observer;0.0.0.0:2181',
            ]
            data, _ = zk.reconfig(
              joining=None, leaving=None, new_members=','.join(new))

            zk.stop()

        :raises:
            :exc:`~kazoo.exceptions.UnimplementedError` if not supported.

            :exc:`~kazoo.exceptions.NewConfigNoQuorumError` if no quorum of new
            config is connected and up-to-date with the leader of last
            commmitted config - try invoking reconfiguration after new servers
            are connected and synced.

            :exc:`~kazoo.exceptions.ReconfigInProcessError` if another
            reconfiguration is in progress.

            :exc:`~kazoo.exceptions.BadVersionError` if version doesn't
            match.

            :exc:`~kazoo.exceptions.BadArgumentsError` if any of the given
            lists of servers has a bad format.

            :exc:`~kazoo.exceptions.ZookeeperError` if the server
            returns a non-zero error code.

        """
        result = self.reconfig_async(joining, leaving, new_members,
                                     from_config)
        return result.get()

    def reconfig_async(self, joining, leaving, new_members, from_config):
        """Asynchronously reconfig a cluster. Takes the same arguments as
        :meth:`reconfig`.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        if joining and not isinstance(joining, string_types):
            raise TypeError("Invalid type for 'joining' (string expected)")
        if leaving and not isinstance(leaving, string_types):
            raise TypeError("Invalid type for 'leaving' (string expected)")
        if new_members and not isinstance(new_members, string_types):
            raise TypeError("Invalid type for 'new_members' (string "
                            "expected)")
        if not isinstance(from_config, int):
            raise TypeError("Invalid type for 'from_config' (int expected)")

        async_result = self.handler.async_result()
        reconfig = Reconfig(joining, leaving, new_members, from_config)
        self._call(reconfig, async_result)

        return async_result


class TransactionRequest(object):
    """A Zookeeper Transaction Request

    A Transaction provides a builder object that can be used to
    construct and commit an atomic set of operations. The transaction
    must be committed before its sent.

    Transactions are not thread-safe and should not be accessed from
    multiple threads at once.

    .. note::

        The ``committed`` attribute only indicates whether this
        transaction has been sent to Zookeeper and is used to prevent
        duplicate commits of the same transaction. The result should be
        checked to determine if the transaction executed as desired.

    .. versionadded:: 0.6
        Requires Zookeeper 3.4+

    """
    def __init__(self, client):
        self.client = client
        self.operations = []
        self.committed = False

    def create(self, path, value=b"", acl=None, ephemeral=False,
               sequence=False):
        """Add a create ZNode to the transaction. Takes the same
        arguments as :meth:`KazooClient.create`, with the exception
        of `makepath`.

        :returns: None

        """
        if acl is None and self.client.default_acl:
            acl = self.client.default_acl

        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if acl and not isinstance(acl, (tuple, list)):
            raise TypeError("Invalid type for 'acl' (acl must be a tuple/list"
                            " of ACL's")
        if not isinstance(value, bytes_types):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if not isinstance(ephemeral, bool):
            raise TypeError("Invalid type for 'ephemeral' (bool expected)")
        if not isinstance(sequence, bool):
            raise TypeError("Invalid type for 'sequence' (bool expected)")

        flags = 0
        if ephemeral:
            flags |= 1
        if sequence:
            flags |= 2
        if acl is None:
            acl = OPEN_ACL_UNSAFE

        self._add(Create(_prefix_root(self.client.chroot, path), value, acl,
                         flags), None)

    def delete(self, path, version=-1):
        """Add a delete ZNode to the transaction. Takes the same
        arguments as :meth:`KazooClient.delete`, with the exception of
        `recursive`.

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(version, int):
            raise TypeError("Invalid type for 'version' (int expected)")
        self._add(Delete(_prefix_root(self.client.chroot, path), version))

    def set_data(self, path, value, version=-1):
        """Add a set ZNode value to the transaction. Takes the same
        arguments as :meth:`KazooClient.set`.

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(value, bytes_types):
            raise TypeError("Invalid type for 'value' (must be a byte string)")
        if not isinstance(version, int):
            raise TypeError("Invalid type for 'version' (int expected)")
        self._add(SetData(_prefix_root(self.client.chroot, path), value,
                  version))

    def check(self, path, version):
        """Add a Check Version to the transaction.

        This command will fail and abort a transaction if the path
        does not match the specified version.

        """
        if not isinstance(path, string_types):
            raise TypeError("Invalid type for 'path' (string expected)")
        if not isinstance(version, int):
            raise TypeError("Invalid type for 'version' (int expected)")
        self._add(CheckVersion(_prefix_root(self.client.chroot, path),
                  version))

    def commit_async(self):
        """Commit the transaction asynchronously.

        :rtype: :class:`~kazoo.interfaces.IAsyncResult`

        """
        self._check_tx_state()
        self.committed = True
        async_object = self.client.handler.async_result()
        self.client._call(Transaction(self.operations), async_object)
        return async_object

    def commit(self):
        """Commit the transaction.

        :returns: A list of the results for each operation in the
                  transaction.

        """
        return self.commit_async().get()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        """Commit and cleanup accumulated transaction data."""
        if not exc_type:
            self.commit()

    def _check_tx_state(self):
        if self.committed:
            raise ValueError('Transaction already committed')

    def _add(self, request, post_processor=None):
        self._check_tx_state()
        self.client.logger.log(BLATHER, 'Added %r to %r', request, self)
        self.operations.append(request)
