# Copyright 2009 Shikhar Bhushan
# Copyright 2011 Leonidas Poulopoulos
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module is a thin layer of abstraction around the library.
It exposes all core functionality.
"""

from ncclient import operations
from ncclient import transport
import socket
import logging
import functools

from ncclient.xml_ import *

logger = logging.getLogger('ncclient.manager')

OPERATIONS = {
    "get": operations.Get,
    "get_config": operations.GetConfig,
    "get_schema": operations.GetSchema,
    "dispatch": operations.Dispatch,
    "edit_config": operations.EditConfig,
    "copy_config": operations.CopyConfig,
    "validate": operations.Validate,
    "commit": operations.Commit,
    "discard_changes": operations.DiscardChanges,
    "cancel_commit": operations.CancelCommit,
    "delete_config": operations.DeleteConfig,
    "lock": operations.Lock,
    "unlock": operations.Unlock,
    "create_subscription": operations.CreateSubscription,
    "close_session": operations.CloseSession,
    "kill_session": operations.KillSession,
    "poweroff_machine": operations.PoweroffMachine,
    "reboot_machine": operations.RebootMachine,
    "rpc": operations.GenericRPC,
}

"""
Dictionary of base method names and corresponding :class:`~ncclient.operations.RPC`
subclasses. It is used to lookup operations, e.g. `get_config` is mapped to
:class:`~ncclient.operations.GetConfig`. It is thus possible to add additional
operations to the :class:`Manager` API.
"""


def make_device_handler(device_params, ignore_errors=None):
    """
    Create a device handler object that provides device specific parameters and
    functions, which are called in various places throughout our code.

    If no device_params are defined or the "name" in the parameter dict is not
    known then a default handler will be returned.

    """
    if device_params is None:
        device_params = {}

    handler = device_params.get('handler', None)
    if handler:
        return handler(device_params, ignore_errors)

    device_name = device_params.get("name", "default")
    # Attempt to import device handler class. All device handlers are
    # in a module called "ncclient.devices.<devicename>" and in a class named
    # "<devicename>DeviceHandler", with the first letter capitalized.
    class_name          = "%sDeviceHandler" % device_name.capitalize()
    devices_module_name = "ncclient.devices.%s" % device_name
    dev_module_obj      = __import__(devices_module_name)
    handler_module_obj  = getattr(getattr(dev_module_obj, "devices"), device_name)
    class_obj           = getattr(handler_module_obj, class_name)
    handler_obj         = class_obj(device_params, ignore_errors)
    return handler_obj


def _extract_device_params(kwds):
    device_params = kwds.pop("device_params", None)

    return device_params

def _extract_errors_params(kwds):
    errors_params = kwds.pop("errors_params", {})
    return (
        errors_params.get("ignore_errors", []),
        errors_params.get("raise_mode", operations.RaiseMode.ALL)
    )

def _extract_manager_params(kwds):
    manager_params = kwds.pop("manager_params", {})

    # To maintain backward compatibility
    if 'timeout' not in manager_params and 'timeout' in kwds:
        manager_params['timeout'] = kwds['timeout']
    return manager_params

def _extract_nc_params(kwds):
    nc_params = kwds.pop("nc_params", {})

    return nc_params

def connect_ssh(*args, **kwds):
    """Initialize a :class:`Manager` over the SSH transport.
    For documentation of arguments see :meth:`ncclient.transport.SSHSession.connect`.

    The underlying :class:`ncclient.transport.SSHSession` is created with
    :data:`CAPABILITIES`. All the provided arguments are passed directly to its
    implementation of :meth:`~ncclient.transport.SSHSession.connect`.

    To customize the :class:`Manager`, add a `manager_params` dictionary in connection
    parameters (e.g. `manager_params={'timeout': 60}` for a bigger RPC timeout parameter)

    To invoke advanced vendor related operation add
    `device_params={'name': '<vendor_alias>'}` in connection parameters. For the time,
    'junos' and 'nexus' are supported for Juniper and Cisco Nexus respectively.

    A custom device handler can be provided with
    `device_params={'handler':<handler class>}` in connection parameters.

    To specify errors to be be ignored in the RPC reply add
    `errors_params={'ignore_errors': ['error pattern to ignore']}` in connection parameters.

    To control the error raising mode add `raise_mode` in the `errors_params`
    (e.g. `errors_params={'raise_mode': 0}` for ignoring all RPC errors)
    See :class:`ncclient.operations.rpc.RaiseMode` for valid values of `raise_mode`

    """
    # Extract device/manager/netconf parameter dictionaries, if they were passed into this function.
    # Remove them from kwds (which should keep only session.connect() parameters).
    device_params = _extract_device_params(kwds)
    manager_params = _extract_manager_params(kwds)
    nc_params = _extract_nc_params(kwds)
    ignore_errors, raise_mode = _extract_errors_params(kwds)
    manager_params["raise_mode"] = raise_mode

    device_handler = make_device_handler(device_params, ignore_errors)
    device_handler.add_additional_ssh_connect_params(kwds)
    device_handler.add_additional_netconf_params(nc_params)
    session = transport.SSHSession(device_handler)

    try:
       session.connect(*args, **kwds)
    except Exception as ex:
        if session.transport:
            session.close()
        raise
    return Manager(session, device_handler, **manager_params)

def connect_libssh(*args, **kwargs):
    """Initialize a :class:`Manager` over the LibSSH transport."""
    if not hasattr(transport, 'libssh'):
        raise ValueError("LibSSH transport is not available, install 'ssh-python' package.")

    device_params = _extract_device_params(kwargs)
    manager_params = _extract_manager_params(kwargs)
    nc_params = _extract_nc_params(kwargs)
    ignore_errors, raise_mode = _extract_errors_params(kwargs)
    manager_params["raise_mode"] = raise_mode

    device_handler = make_device_handler(device_params, ignore_errors)
    device_handler.add_additional_ssh_connect_params(kwargs)
    device_handler.add_additional_netconf_params(nc_params)
    session = transport.libssh.LibSSHSession(device_handler)

    session.connect(*args, **kwargs)
    return Manager(session, device_handler, **manager_params)

def connect_tls(*args, **kwargs):
    """Initialize a :class:`Manager` over the TLS transport."""
    device_params = _extract_device_params(kwargs)
    manager_params = _extract_manager_params(kwargs)
    nc_params = _extract_nc_params(kwargs)
    ignore_errors, raise_mode = _extract_errors_params(kwargs)
    manager_params["raise_mode"] = raise_mode

    device_handler = make_device_handler(device_params, ignore_errors)
    device_handler.add_additional_netconf_params(nc_params)
    session = transport.TLSSession(device_handler)

    session.connect(*args, **kwargs)

    return Manager(session, device_handler, **manager_params)

def connect_uds(*args, **kwargs):
    """Initialize a :class:`Manager` over the Unix Socket transport."""
    device_params = _extract_device_params(kwargs)
    manager_params = _extract_manager_params(kwargs)
    nc_params = _extract_nc_params(kwargs)
    ignore_errors, raise_mode = _extract_errors_params(kwargs)
    manager_params["raise_mode"] = raise_mode

    device_handler = make_device_handler(device_params, ignore_errors)
    device_handler.add_additional_netconf_params(nc_params)
    session = transport.UnixSocketSession(device_handler)

    session.connect(*args, **kwargs)

    return Manager(session, device_handler, **manager_params)

def connect_ioproc(*args, **kwds):
    device_params = _extract_device_params(kwds)
    manager_params = _extract_manager_params(kwds)
    ignore_errors, raise_mode = _extract_errors_params(kwds)
    manager_params["raise_mode"] = raise_mode

    if device_params:
        import_string = 'ncclient.transport.third_party.'
        import_string += device_params['name'] + '.ioproc'
        third_party_import = __import__(import_string, fromlist=['IOProc'])

    device_handler = make_device_handler(device_params, ignore_errors)

    session = third_party_import.IOProc(device_handler)
    session.connect()

    return Manager(session, device_handler, **manager_params)


def connect(*args, **kwds):
    if "host" in kwds:
        host = kwds["host"]
        device_params = kwds.get('device_params', {})
        if host == 'localhost' and device_params.get('name') == 'junos' \
                and device_params.get('local'):
            return connect_ioproc(*args, **kwds)
        elif kwds.pop("use_libssh", False):
            return connect_libssh(*args, **kwds)
        else:
            return connect_ssh(*args, **kwds)

def call_home(*args, **kwds):
    host = kwds["host"]
    port = kwds.get("port",4334)
    srv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv_socket.bind((host, port))
    srv_socket.settimeout(kwds.get("timeout", 10))
    srv_socket.listen()

    sock, remote_host = srv_socket.accept()
    logger.info('Callhome connection initiated from remote host {0}'.format(remote_host))
    kwds['sock'] = sock
    return connect_ssh(*args, **kwds)

class Manager:

    """
    For details on the expected behavior of the operations and their
        parameters refer to :rfc:`6241`.

    Manager instances are also context managers so you can use it like this::

        with manager.connect("host") as m:
            # do your stuff

    ... or like this::

        m = manager.connect("host")
        try:
            # do your stuff
        finally:
            m.close_session()
    """

   # __metaclass__ = OpExecutor


    HUGE_TREE_DEFAULT = False
    """Default for `huge_tree` support for XML parsing of RPC replies (defaults to False)"""

    def __init__(self, session, device_handler, timeout=30, raise_mode=operations.RaiseMode.ALL):
        self._session = session
        self._async_mode = False
        self._timeout = timeout
        self._raise_mode = raise_mode
        self._huge_tree = self.HUGE_TREE_DEFAULT
        self._device_handler = device_handler
        self._vendor_operations = {}
        if device_handler:
            self._vendor_operations.update(device_handler.add_additional_operations())

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close_session()
        return False

    def __set_timeout(self, timeout):
        self._timeout = timeout

    def __set_async_mode(self, mode):
        self._async_mode = mode

    def __set_raise_mode(self, mode):
        assert(mode in (operations.RaiseMode.NONE, operations.RaiseMode.ERRORS, operations.RaiseMode.ALL))
        self._raise_mode = mode

    def execute(self, cls, *args, **kwds):
        return cls(self._session,
                   device_handler=self._device_handler,
                   async_mode=self._async_mode,
                   timeout=self._timeout,
                   raise_mode=self._raise_mode,
                   huge_tree=self._huge_tree).request(*args, **kwds)

    def locked(self, target):
        """Returns a context manager for a lock on a datastore, where
        *target* is the name of the configuration datastore to lock, e.g.::

            with m.locked("running"):
                # do your stuff

        ... instead of::

            m.lock("running")
            try:
                # do your stuff
            finally:
                m.unlock("running")
        """
        return operations.LockContext(self._session, self._device_handler, target)

    def scp(self):
        return self._session.scp()

    def session(self):
        raise NotImplementedError

    def __getattr__(self, method):
        if method in self._vendor_operations:
            return functools.partial(self.execute, self._vendor_operations[method])
        elif method in OPERATIONS:
            return functools.partial(self.execute, OPERATIONS[method])
        else:
            """Parse args/kwargs correctly in order to build XML element"""
            def _missing(*args, **kwargs):
                m = method.replace('_', '-')
                root = new_ele(m)
                if args:
                    for arg in args:
                        sub_ele(root, arg)
                r = self.rpc(root)
                return r
            return _missing

    def take_notification(self, block=True, timeout=None):
        """Attempt to retrieve one notification from the queue of received
        notifications.

        If block is True, the call will wait until a notification is
        received.

        If timeout is a number greater than 0, the call will wait that
        many seconds to receive a notification before timing out.

        If there is no notification available when block is False or
        when the timeout has elapse, None will be returned.

        Otherwise a :class:`~ncclient.operations.notify.Notification`
        object will be returned.
        """
        return self._session.take_notification(block, timeout)

    @property
    def client_capabilities(self):
        """:class:`~ncclient.capabilities.Capabilities` object representing
        the client's capabilities."""
        return self._session._client_capabilities

    @property
    def server_capabilities(self):
        """:class:`~ncclient.capabilities.Capabilities` object representing
        the server's capabilities."""
        return self._session._server_capabilities

    @property
    def channel_id(self):
        return self._session._channel_id

    @property
    def channel_name(self):
        return self._session._channel_name

    @property
    def session_id(self):
        """`session-id` assigned by the NETCONF server."""
        return self._session.id

    @property
    def connected(self):
        """Whether currently connected to the NETCONF server."""
        return self._session.connected

    async_mode = property(fget=lambda self: self._async_mode,
                          fset=__set_async_mode)
    """Specify whether operations are executed asynchronously (`True`) or
    synchronously (`False`) (the default)."""

    timeout = property(fget=lambda self: self._timeout, fset=__set_timeout)
    """Specify the timeout for synchronous RPC requests."""

    raise_mode = property(fget=lambda self: self._raise_mode,
                          fset=__set_raise_mode)
    """Specify which errors are raised as :exc:`~ncclient.operations.RPCError`
    exceptions. Valid values are the constants defined in
    :class:`~ncclient.operations.RaiseMode`.
    The default value is :attr:`~ncclient.operations.RaiseMode.ALL`."""

    @property
    def huge_tree(self):
        """Whether `huge_tree` support for XML parsing of RPC replies is enabled (default=False)
        The default value is configurable through :attr:`~ncclient.manager.Manager.HUGE_TREE_DEFAULT`"""
        return self._huge_tree

    @huge_tree.setter
    def huge_tree(self, x):
        self._huge_tree = x
