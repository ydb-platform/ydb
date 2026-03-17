# Copyright (c) 2016-2020 Canonical Ltd
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
import collections
import os
import stat
import time

import six
from six.moves.urllib import parse
try:
    from ws4py.client import WebSocketBaseClient
    from ws4py.manager import WebSocketManager
    from ws4py.messaging import BinaryMessage
    _ws4py_installed = True
except ImportError:  # pragma: no cover
    WebSocketBaseClient = object
    _ws4py_installed = False

from pylxd import managers
from pylxd.exceptions import LXDAPIException
from pylxd.models import _model as model
from pylxd.models.operation import Operation

if six.PY2:
    # Python2.7 doesn't have this natively
    from pylxd.exceptions import NotADirectoryError


class InstanceState(object):
    """A simple object for representing instance state."""

    def __init__(self, **kwargs):
        for key, value in six.iteritems(kwargs):
            setattr(self, key, value)


_InstanceExecuteResult = collections.namedtuple(
    'InstanceExecuteResult',
    ['exit_code', 'stdout', 'stderr'])


class Instance(model.Model):
    """An LXD Instance.

    This class is not intended to be used directly, but rather to be used
    via `Client.instance.create`.
    """

    architecture = model.Attribute()
    config = model.Attribute()
    created_at = model.Attribute()
    devices = model.Attribute()
    ephemeral = model.Attribute()
    expanded_config = model.Attribute()
    expanded_devices = model.Attribute()
    name = model.Attribute(readonly=True)
    description = model.Attribute()
    profiles = model.Attribute()
    status = model.Attribute(readonly=True)
    last_used_at = model.Attribute(readonly=True)
    location = model.Attribute(readonly=True)
    type = model.Attribute(readonly=True)

    status_code = model.Attribute(readonly=True)
    stateful = model.Attribute(readonly=True)

    snapshots = model.Manager()
    files = model.Manager()

    _endpoint = 'instances'

    @property
    def api(self):
        return self.client.api[self._endpoint][self.name]

    class FilesManager(object):
        """A pseudo-manager for namespacing file operations."""

        def __init__(self, instance):
            self._instance = instance
            self._endpoint = instance.client.api[
                instance._endpoint][instance.name].files

        def put(self, filepath, data, mode=None, uid=None, gid=None):
            """Push a file to the instance.

            This pushes a single file to the instances file system named by
            the `filepath`.

            :param filepath: The path in the instance to to store the data in.
            :type filepath: str
            :param data: The data to store in the file.
            :type data: bytes or str
            :param mode: The unit mode to store the file with.  The default of
                None stores the file with the current mask of 0700, which is
                the lxd default.
            :type mode: Union[oct, int, str]
            :param uid: The uid to use inside the instance. Default of None
                results in 0 (root).
            :type uid: int
            :param gid: The gid to use inside the instance.  Default of None
                results in 0 (root).
            :type gid: int
            :raises: LXDAPIException if something goes wrong
            """
            headers = self._resolve_headers(mode=mode, uid=uid, gid=gid)
            response = self._endpoint.post(
                params={'path': filepath},
                data=data,
                headers=headers or None)
            if response.status_code == 200:
                return
            raise LXDAPIException(response)

        @staticmethod
        def _resolve_headers(headers=None, mode=None, uid=None, gid=None):
            if headers is None:
                headers = {}
            if mode is not None:
                if isinstance(mode, int):
                    mode = format(mode, 'o')
                if not isinstance(mode, six.string_types):
                    raise ValueError("'mode' parameter must be int or string")
                if not mode.startswith('0'):
                    mode = '0{}'.format(mode)
                headers['X-LXD-mode'] = mode
            if uid is not None:
                headers['X-LXD-uid'] = str(uid)
            if gid is not None:
                headers['X-LXD-gid'] = str(gid)
            return headers

        def delete_available(self):
            """File deletion is an extension API and may not be available.
            https://github.com/lxc/lxd/blob/master/doc/api-extensions.md#file_delete
            """
            return self._instance.client.has_api_extension('file_delete')

        def delete(self, filepath):
            self._instance.client.assert_has_api_extension('file_delete')
            response = self._endpoint.delete(params={'path': filepath})
            if response.status_code != 200:
                raise LXDAPIException(response)

        def get(self, filepath):
            response = self._endpoint.get(
                params={'path': filepath}, is_api=False)
            return response.content

        def recursive_put(self, src, dst, mode=None, uid=None, gid=None):
            """Recursively push directory to the instance.

            Recursively pushes directory to the instances
            named by the `dst`

            :param src: The source path of directory to copy.
            :type src: str
            :param dst: The destination path in the instance
                    of directory to copy
            :type dst: str
            :param mode: The unit mode to store the file with.  The default of
                None stores the file with the current mask of 0700, which is
                the lxd default.
            :type mode: Union[oct, int, str]
            :param uid: The uid to use inside the instance. Default of None
                results in 0 (root).
            :type uid: int
            :param gid: The gid to use inside the instance.  Default of None
                results in 0 (root).
            :type gid: int
            :raises: NotADirectoryError if src is not a directory
            :raises: LXDAPIException if an error occurs
            """
            norm_src = os.path.normpath(src)
            if not os.path.isdir(norm_src):
                raise NotADirectoryError(
                    "'src' parameter must be a directory "
                )

            idx = len(norm_src)
            dst_items = set()

            for path, dirname, files in os.walk(norm_src):
                dst_path = os.path.normpath(
                    os.path.join(dst, path[idx:].lstrip(os.path.sep)))
                # create directory or symlink (depending on what's there)
                if path not in dst_items:
                    dst_items.add(path)
                    headers = self._resolve_headers(mode=mode,
                                                    uid=uid, gid=gid)
                    # determine what the file is: a directory or a symlink
                    fmode = os.stat(path).st_mode
                    if stat.S_ISLNK(fmode):
                        headers['X-LXD-type'] = 'symlink'
                    else:
                        headers['X-LXD-type'] = 'directory'
                    self._endpoint.post(
                        params={'path': dst_path},
                        headers=headers)

                # copy files
                for f in files:
                    src_file = os.path.join(path, f)
                    with open(src_file, 'rb') as fp:
                        filepath = os.path.join(dst_path, f)
                        headers = self._resolve_headers(mode=mode,
                                                        uid=uid,
                                                        gid=gid)
                        response = self._endpoint.post(
                            params={'path': filepath},
                            data=fp.read(),
                            headers=headers or None)
                        if response.status_code != 200:
                            raise LXDAPIException(response)

    @classmethod
    def exists(cls, client, name):
        """Determine whether a instance exists."""
        try:
            getattr(client, cls._endpoint).get(name)
            return True
        except cls.NotFound:
            return False

    @classmethod
    def get(cls, client, name):
        """Get a instance by name."""
        response = client.api[cls._endpoint][name].get()

        return cls(client, **response.json()['metadata'])

    @classmethod
    def all(cls, client):
        """Get all instances.

        Instances returned from this method will only have the name
        set, as that is the only property returned from LXD. If more
        information is needed, `Instance.sync` is the method call
        that should be used.
        """
        response = client.api[cls._endpoint].get()

        instances = []
        for url in response.json()['metadata']:
            name = url.split('/')[-1]
            instances.append(cls(client, name=name))
        return instances

    @classmethod
    def create(cls, client, config, wait=False, target=None):
        """Create a new instance config.

        :param client: client instance
        :type client: Client
        :param config: The configuration for the new instance.
        :type config: dict
        :param wait: Whether to wait for async operations to complete.
        :type wait: bool
        :param target: If in cluster mode, the target member.
        :type target: str
        :raises LXDAPIException: if something goes wrong.
        :returns: an instance if successful
        :rtype: :class:`Instance`
        """
        response = client.api[cls._endpoint].post(
            json=config, target=target)

        if wait:
            client.operations.wait_for_operation(response.json()['operation'])
        return cls(client, name=config['name'])

    def __init__(self, *args, **kwargs):
        super(Instance, self).__init__(*args, **kwargs)

        self.snapshots = managers.SnapshotManager(self.client, self)
        self.files = self.FilesManager(self)

    def rename(self, name, wait=False):
        """Rename an instance."""
        response = self.api.post(json={'name': name})

        if wait:
            self.client.operations.wait_for_operation(
                response.json()['operation'])
        self.name = name

    def _set_state(self, state, timeout=30, force=True, wait=False):
        response = self.api.state.put(json={
            'action': state,
            'timeout': timeout,
            'force': force
        })
        if wait:
            self.client.operations.wait_for_operation(
                response.json()['operation'])
            if 'status' in self.__dirty__:
                self.__dirty__.remove('status')
            if self.ephemeral and state == 'stop':
                self.client = None
            else:
                self.sync()

    def state(self):
        response = self.api.state.get()
        state = InstanceState(**response.json()['metadata'])
        return state

    def start(self, timeout=30, force=True, wait=False):
        """Start the instance."""
        return self._set_state('start',
                               timeout=timeout,
                               force=force,
                               wait=wait)

    def stop(self, timeout=30, force=True, wait=False):
        """Stop the instance."""
        return self._set_state('stop',
                               timeout=timeout,
                               force=force,
                               wait=wait)

    def restart(self, timeout=30, force=True, wait=False):
        """Restart the instance."""
        return self._set_state('restart',
                               timeout=timeout,
                               force=force,
                               wait=wait)

    def freeze(self, timeout=30, force=True, wait=False):
        """Freeze the instance."""
        return self._set_state('freeze',
                               timeout=timeout,
                               force=force,
                               wait=wait)

    def unfreeze(self, timeout=30, force=True, wait=False):
        """Unfreeze the instance."""
        return self._set_state('unfreeze',
                               timeout=timeout,
                               force=force,
                               wait=wait)

    def execute(
            self, commands, environment=None, encoding=None, decode=True,
            stdin_payload=None, stdin_encoding="utf-8",
            stdout_handler=None, stderr_handler=None):
        """Execute a command on the instance. stdout and stderr are buffered if
        no handler is given.

        :param commands: The command and arguments as a list of strings
        :type commands: [str]
        :param environment: The environment variables to pass with the command
        :type environment: {str: str}
        :param encoding: The encoding to use for stdout/stderr if the param
            decode is True.  If encoding is None, then no override is
            performed and whatever the existing encoding from LXD is used.
        :type encoding: str
        :param decode: Whether to decode the stdout/stderr or just return the
            raw buffers.
        :type decode: bool
        :param stdin_payload: Payload to pass via stdin
        :type stdin_payload: Can be a file, string, bytearray, generator or
            ws4py Message object
        :param stdin_encoding: Encoding to pass text to stdin (default utf-8)
        :param stdout_handler: Callable than receive as first parameter each
            message received via stdout
        :type stdout_handler: Callable[[str], None]
        :param stderr_handler: Callable than receive as first parameter each
            message received via stderr
        :type stderr_handler: Callable[[str], None]
        :raises ValueError: if the ws4py library is not installed.
        :returns: A tuple of `(exit_code, stdout, stderr)`
        :rtype: _InstanceExecuteResult() namedtuple
        """
        if not _ws4py_installed:
            raise ValueError(
                'This feature requires the optional ws4py library.')
        if isinstance(commands, six.string_types):
            raise TypeError("First argument must be a list.")
        if environment is None:
            environment = {}

        response = self.api['exec'].post(json={
            'command': commands,
            'environment': environment,
            'wait-for-websocket': True,
            'interactive': False,
        })

        fds = response.json()['metadata']['metadata']['fds']
        operation_id = \
            Operation.extract_operation_id(response.json()['operation'])
        parsed = parse.urlparse(
            self.client.api.operations[operation_id].websocket._api_endpoint)

        with managers.web_socket_manager(WebSocketManager()) as manager:
            stdin = _StdinWebsocket(
                self.client.websocket_url, payload=stdin_payload,
                encoding=stdin_encoding
            )
            stdin.resource = '{}?secret={}'.format(parsed.path, fds['0'])
            stdin.connect()
            stdout = _CommandWebsocketClient(
                manager, self.client.websocket_url,
                encoding=encoding, decode=decode, handler=stdout_handler)
            stdout.resource = '{}?secret={}'.format(parsed.path, fds['1'])
            stdout.connect()
            stderr = _CommandWebsocketClient(
                manager, self.client.websocket_url,
                encoding=encoding, decode=decode, handler=stderr_handler)
            stderr.resource = '{}?secret={}'.format(parsed.path, fds['2'])
            stderr.connect()

            manager.start()

            # watch for the end of the command:
            while True:
                operation = self.client.operations.get(operation_id)
                if 'return' in operation.metadata:
                    break
                time.sleep(.5)  # pragma: no cover

            try:
                stdin.close()
            except BrokenPipeError:
                pass

            stdout.finish_soon()
            stderr.finish_soon()
            try:
                manager.close_all()
            except BrokenPipeError:
                pass

            while not stdout.finished or not stderr.finished:
                time.sleep(.1)  # progma: no cover

            manager.stop()
            manager.join()

            return _InstanceExecuteResult(
                operation.metadata['return'], stdout.data, stderr.data)

    def raw_interactive_execute(self, commands, environment=None):
        """Execute a command on the instance interactively and returns
        urls to websockets. The urls contain a secret uuid, and can be accesses
        without further authentication. The caller has to open and manage
        the websockets themselves.

        :param commands: The command and arguments as a list of strings
           (most likely a shell)
        :type commands: [str]
        :param environment: The environment variables to pass with the command
        :type environment: {str: str}
        :returns: Two urls to an interactive websocket and a control socket
        :rtype: {'ws':str,'control':str}
        """
        if isinstance(commands, six.string_types):
            raise TypeError("First argument must be a list.")

        if environment is None:
            environment = {}

        response = self.api['exec'].post(json={
            'command': commands,
            'environment': environment,
            'wait-for-websocket': True,
            'interactive': True,
        })

        fds = response.json()['metadata']['metadata']['fds']
        operation_id = response.json()['operation']\
            .split('/')[-1].split('?')[0]
        parsed = parse.urlparse(
            self.client.api.operations[operation_id].websocket._api_endpoint)

        return {'ws': '{}?secret={}'.format(parsed.path, fds['0']),
                'control': '{}?secret={}'.format(parsed.path, fds['control'])}

    def migrate(self, new_client, live=False, wait=False):
        """Migrate a instance.

        Destination host information is contained in the client
        connection passed in.

        If the `live` param is True, then a live migration is attempted,
        otherwise a non live one is running.

        If the instance is running for live migration, it either must be shut
        down first or criu must be installed on the source and destination
        machines and the `live` param should be True.

        :param new_client: the pylxd client connection to migrate the instance
            to.
        :type new_client: :class:`pylxd.client.Client`
        :param live: whether to perform a live migration
        :type live: bool
        :param wait: if True, wait for the migration to complete
        :type wait: bool
        :raises: LXDAPIException if any of the API calls fail.
        :raises: ValueError if source of target are local connections
        :returns: the response from LXD of the new instance (the target of the
            migration and not the operation if waited on.)
        :rtype: :class:`requests.Response`
        """
        if self.api.scheme in ('http+unix',):
            raise ValueError('Cannot migrate from a local client connection')

        if self.status_code == 103:
            try:
                res = getattr(new_client, self._endpoint).create(
                    self.generate_migration_data(live), wait=wait)
            except LXDAPIException as e:
                if e.response.status_code == 103:
                    self.delete()
                    return getattr(new_client, self._endpoint).get(self.name)
                else:
                    raise e
        else:
            res = getattr(new_client, self._endpoint).create(
                self.generate_migration_data(live), wait=wait)
        self.delete()
        return res

    def generate_migration_data(self, live=False):
        """Generate the migration data.

        This method can be used to handle migrations where the client
        connection uses the local unix socket. For more information on
        migration, see `Instance.migrate`.

        :param live: Whether to include "live": "true" in the migration
        :type live: bool
        :raises: LXDAPIException if the request to migrate fails
        :returns: dictionary of migration data suitable to send to an new
            client to complete a migration.
        :rtype: Dict[str, ANY]
        """
        self.sync()  # Make sure the object isn't stale
        _json = {'migration': True}
        if live:
            _json['live'] = True
        response = self.api.post(json=_json)
        operation = self.client.operations.get(response.json()['operation'])
        operation_url = self.client.api.operations[operation.id]._api_endpoint
        secrets = response.json()['metadata']['metadata']
        cert = self.client.host_info['environment']['certificate']

        return {
            'name': self.name,
            'architecture': self.architecture,
            'config': self.config,
            'devices': self.devices,
            'epehemeral': self.ephemeral,
            'default': self.profiles,
            'source': {
                'type': 'migration',
                'operation': operation_url,
                'mode': 'pull',
                'certificate': cert,
                'secrets': secrets,
            }
        }

    def publish(self, public=False, wait=False):
        """Publish a instance as an image.

        The instance must be stopped in order publish it as an image. This
        method does not enforce that constraint, so a LXDAPIException may be
        raised if this method is called on a running instance.

        If wait=True, an Image is returned.
        """
        data = {
            'public': public,
            'source': {
                'type': self.type,
                'name': self.name,
            }
        }

        response = self.client.api.images.post(json=data)
        if wait:
            operation = self.client.operations.wait_for_operation(
                response.json()['operation'])

            return self.client.images.get(operation.metadata['fingerprint'])

    def restore_snapshot(self, snapshot_name, wait=False):
        """Restore a snapshot using its name.

        Attempts to restore a instance using a snapshot previously made.  The
        instance should be stopped, but the method does not enforce this
        constraint, so an LXDAPIException may be raised if this method fails.

        :param snapshot_name: the name of the snapshot to restore from
        :type snapshot_name: str
        :param wait: wait until the operation is completed.
        :type wait: boolean
        :raises: LXDAPIException if the the operation fails.
        :returns: the original response from the restore operation (not the
            operation result)
        :rtype: :class:`requests.Response`
        """
        response = self.api.put(json={"restore": snapshot_name})
        if wait:
            self.client.operations.wait_for_operation(
                response.json()['operation'])
        return response


class _CommandWebsocketClient(WebSocketBaseClient):  # pragma: no cover
    """Handle a websocket for instance.execute(...) and manage decoding of the
    returned values depending on 'decode' and 'encoding' parameters.
    """

    def __init__(self, manager, *args, **kwargs):
        self.manager = manager
        self.decode = kwargs.pop('decode', True)
        self.encoding = kwargs.pop('encoding', None)
        self.handler = kwargs.pop('handler', None)
        self.message_encoding = None
        self.finish_off = False
        self.finished = False
        self.last_message_empty = False
        self.buffer = []
        super(_CommandWebsocketClient, self).__init__(*args, **kwargs)

    def handshake_ok(self):
        self.manager.add(self)
        self.buffer = []

    def received_message(self, message):
        if message.data is None or len(message.data) == 0:
            self.last_message_empty = True
            if self.finish_off:
                self.finished = True
            return
        else:
            self.last_message_empty = False
        if message.encoding and self.message_encoding is None:
            self.message_encoding = message.encoding
        if self.handler:
            self.handler(self._maybe_decode(message.data))
        else:
            self.buffer.append(message.data)
        if self.finish_off and isinstance(message, BinaryMessage):
            self.finished = True

    def closed(self, code, reason=None):
        self.finished = True

    def finish_soon(self):
        self.finish_off = True
        if self.last_message_empty:
            self.finished = True

    def _maybe_decode(self, buffer):
        if self.decode and buffer is not None:
            if self.encoding:
                return buffer.decode(self.encoding)
            if self.message_encoding:
                return buffer.decode(self.message_encoding)
            # This is the backwards compatible "always decode to utf-8"
            return buffer.decode('utf-8')
        return buffer

    @property
    def data(self):
        buffer = b''.join(self.buffer)
        return self._maybe_decode(buffer)


class _StdinWebsocket(WebSocketBaseClient):  # pragma: no cover
    """A websocket client for handling stdin.

    Allow comunicate with instance commands via stdin
    """

    def __init__(self, url, payload=None, **kwargs):
        self.encoding = kwargs.pop('encoding', None)
        self.payload = payload
        super(_StdinWebsocket, self).__init__(url, **kwargs)

    def _smart_encode(self, msg):
        if type(msg) == six.text_type and self.encoding:
            return msg.encode(self.encoding)
        return msg

    def handshake_ok(self):
        if self.payload:
            if hasattr(self.payload, "read"):
                self.send(
                    (self._smart_encode(line) for line in self.payload),
                    binary=True
                )
            else:
                self.send(self._smart_encode(self.payload), binary=True)
        self.send(b"", binary=False)


class Snapshot(model.Model):
    """A instance snapshot."""

    name = model.Attribute()
    created_at = model.Attribute()
    stateful = model.Attribute()

    instance = model.Parent()

    @property
    def api(self):
        return self.client.api[self.instance._endpoint][
            self.instance.name].snapshots[self.name]

    @classmethod
    def get(cls, client, instance, name):
        response = client.api[instance._endpoint][
            instance.name].snapshots[name].get()

        snapshot = cls(
            client, instance=instance,
            **response.json()['metadata'])
        # Snapshot names are namespaced in LXD, as
        # instance-name/snapshot-name. We hide that implementation
        # detail.
        snapshot.name = snapshot.name.split('/')[-1]
        return snapshot

    @classmethod
    def all(cls, client, instance):
        response = client.api[instance._endpoint][
            instance.name].snapshots.get()

        return [cls(
                client, name=snapshot.split('/')[-1],
                instance=instance)
                for snapshot in response.json()['metadata']]

    @classmethod
    def create(cls, client, instance, name, stateful=False, wait=False):
        response = client.api[instance._endpoint][
            instance.name].snapshots.post(
                json={'name': name, 'stateful': stateful})

        snapshot = cls(client, instance=instance, name=name)
        if wait:
            client.operations.wait_for_operation(response.json()['operation'])
        return snapshot

    def rename(self, new_name, wait=False):
        """Rename a snapshot."""
        response = self.api.post(json={'name': new_name})
        if wait:
            self.client.operations.wait_for_operation(
                response.json()['operation'])
        self.name = new_name

    def publish(self, public=False, wait=False):
        """Publish a snapshot as an image.

        If wait=True, an Image is returned.

        This functionality is currently broken in LXD. Please see
        https://github.com/lxc/lxd/issues/2201 - The implementation
        here is mostly a guess. Once that bug is fixed, we can verify
        that this works, or file a bug to fix it appropriately.
        """
        data = {
            'public': public,
            'source': {
                'type': 'snapshot',
                'name': '{}/{}'.format(self.instance.name, self.name),
            }
        }

        response = self.client.api.images.post(json=data)
        if wait:
            operation = self.client.operations.wait_for_operation(
                response.json()['operation'])
            return self.client.images.get(operation.metadata['fingerprint'])

    def restore(self, wait=False):
        """Restore this snapshot.

        Attempts to restore a instance using this snapshot.  The instance
        should be stopped, but the method does not enforce this constraint, so
        an LXDAPIException may be raised if this method fails.

        :param wait: wait until the operation is completed.
        :type wait: boolean
        :raises: LXDAPIException if the the operation fails.
        :returns: the original response from the restore operation (not the
            operation result)
        :rtype: :class:`requests.Response`
        """
        return self.instance.restore_snapshot(self.name, wait)
