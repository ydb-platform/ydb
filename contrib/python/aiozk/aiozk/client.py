import asyncio
import logging
import typing

from aiozk import exc, protocol

from .features import Features
from .recipes.proxy import RecipeProxy
from .session import Session
from .transaction import Transaction


log = logging.getLogger(__name__)


class ZKClient:
    """
    The class of Zookeeper Client
    """

    def __init__(
        self,
        servers,
        chroot=None,
        session_timeout=10,
        default_acl=None,
        retry_policy=None,
        allow_read_only=False,
        read_timeout=None,
    ):
        """
        :param str servers: Server list to which ZKClient tries connecting.
            Specify a comma (``,``) separated server list. A server is
            defined as ``address``:``port`` format.


        :param str chroot: Root znode path inside Zookeeper data hierarchy.

        :param float session_timeout: Zookeeper session timeout.

        :param default_acl: Default ACL for .create and .ensure_path coroutines.
            If acl parameter of them is not passed this ACL is used instead.
            If None is passed for default_acl, then ACL for unrestricted access
            is applied. This means that scheme is ``world``, id is ``anyone``
            and all ``READ``/``WRITE``/``CREATE``/``DELETE``/``ADMIN``
            permission bits will be set to the new znode.

        :type default_acl: aiozk.ACL

        :param retry_policy: Retry policy. If None, ``RetryPolicy.forever()``
            is used instead.

        :type retry_policy: aiozk.RetryPolicy

        :param bool allow_read_only: True if you allow this client to make use
            of read only Zookeeper session, otherwise False.

        :param float read_timeout: Timeout on reading from Zookeeper server in
            seconds.
        """
        self.chroot = None
        if chroot:
            self.chroot = self.normalize_path(chroot)
            log.info("Using chroot '%s'", self.chroot)

        self.session = Session(servers, session_timeout, retry_policy, allow_read_only, read_timeout)

        self.default_acl = default_acl or [protocol.UNRESTRICTED_ACCESS]

        self.stat_cache = {}

        self.recipes = RecipeProxy(self)

    def normalize_path(self, path):
        if self.chroot:
            path = '/'.join([self.chroot, path])

        normalized = '/'.join([name for name in path.split('/') if name])

        return '/' + normalized

    def denormalize_path(self, path):
        if self.chroot and path.startswith(self.chroot):
            path = path[len(self.chroot) :]

        return path

    async def start(self):
        """Start Zookeeper session and await for session connected."""
        await self.session.start()

        if self.chroot:
            await self.ensure_path('/')

    @property
    def features(self):
        if self.session.conn:
            return Features(self.session.conn.version_info)
        else:
            return Features((0, 0, 0))

    async def send(self, request):
        response = await self.session.send(request)

        if getattr(request, 'path', None) and getattr(response, 'stat', None):
            self.stat_cache[self.denormalize_path(request.path)] = response.stat

        return response

    async def close(self):
        """Close Zookeeper session and await for session closed."""
        await self.session.close()

    def wait_for_events(self, event_types, path):
        path = self.normalize_path(path)

        loop = asyncio.get_running_loop()
        f = loop.create_future()

        def set_future(_):
            if not f.done():
                f.set_result(None)
            for event_type in event_types:
                self.session.remove_watch_callback(event_type, path, set_future)

        for event_type in event_types:
            self.session.add_watch_callback(event_type, path, set_future)

        return f

    async def exists(self, path, watch=False):
        """
        Check whether the path exists.

        :param str path: Path of znode

        :param bool watch: If True, a watch is set as a side effect

        :return: True if it exists otherwise False
        :rtype: bool
        """
        path = self.normalize_path(path)

        try:
            await self.send(protocol.ExistsRequest(path=path, watch=watch))
        except exc.NoNode:
            return False
        return True

    async def create(
        self,
        path,
        data=None,
        acl=None,
        ephemeral=False,
        sequential=False,
        container=False,
    ):
        """
        Create a znode at the path.

        :param str path: Path of znode

        :param data: Data which will be stored at the znode if the
            request succeeds

        :type data: str, bytes

        :param aiozk.ACL acl: ACL to be set to the new znode.

        :param bool ephemeral: True for creating ephemeral znode otherwise
            False

        :param bool sequential: True for creating seqeuence znode otherwise
            False

        :param bool container: True for creating container znode otherwise
            False

        :return: Path of the created znode
        :rtype: str

        :raises aiozk.exc.NodeExists: Can be raised if a znode at the same path
            already exists.
        """
        if container and not self.features.containers:
            raise ValueError('Cannot create container, feature unavailable.')

        path = self.normalize_path(path)
        acl = acl or self.default_acl

        if self.features.create_with_stat:
            request_class = protocol.Create2Request
        else:
            request_class = protocol.CreateRequest

        request = request_class(path=path, data=data, acl=acl)
        request.set_flags(ephemeral, sequential, container)

        response = await self.send(request)

        return self.denormalize_path(response.path)

    async def ensure_path(self, path, acl=None):
        """
        Ensure all znodes exist in the path. Missing Znodes will be created.

        :param str path: Path of znode

        :param aiozk.ACL acl: ACL to be set to the new znodes

        """
        path = self.normalize_path(path)

        acl = acl or self.default_acl

        paths_to_make = []
        for segment in path[1:].split('/'):
            if not paths_to_make:
                paths_to_make.append('/' + segment)
                continue

            paths_to_make.append('/'.join([paths_to_make[-1], segment]))

        while paths_to_make:
            path = paths_to_make[0]

            if self.features.create_with_stat:
                request = protocol.Create2Request(path=path, acl=acl)
            else:
                request = protocol.CreateRequest(path=path, acl=acl)
            request.set_flags(ephemeral=False, sequential=False, container=self.features.containers)

            try:
                await self.send(request)
            except exc.NodeExists:
                pass

            paths_to_make.pop(0)

    async def delete(self, path, force=False):
        """
        Delete a znode at the path.

        :param str path: Path of znode

        :param bool force: True for ignoring version of the znode. A version of
            a znode is used as an optimistic lock mechanism.
            Set false for making use of a version that is tracked by
            a stat cache of ZKClient.

        :raises aiozk.exc.NoNode: Raised if path does not exist.
        """
        path = self.normalize_path(path)

        if not force and path in self.stat_cache:
            version = self.stat_cache[path].version
        else:
            version = -1

        await self.send(protocol.DeleteRequest(path=path, version=version))

    async def deleteall(self, path):
        """
        Delete all znodes in the path recursively.

        :param str path: Path of znode

        :raises aiozk.exc.NoNode: Raised if path does not exist.
        """
        childs = await self.get_children(path)
        for child in childs:
            await self.deleteall('/'.join([path, child]))
        await self.delete(path, force=True)

    async def get(self, path: str, watch: bool = False) -> typing.Tuple[str, protocol.stat.Stat]:
        """
        Get data as bytes and stat of znode.

        :param str path: Path of znode

        :param bool watch: True for setting a watch event as a side effect,
            otherwise False

        :return: Data and stat of znode
        :rtype: (bytes, aiozk.protocol.stat.Stat)

        :raises aiozk.exc.NoNode: Can be raised if path does not exist
        """
        path = self.normalize_path(path)
        response = await self.send(protocol.GetDataRequest(path=path, watch=watch))

        return (response.data, response.stat)

    async def get_data(self, path, watch=False):
        """
        Get data as bytes.

        :param str path: Path of znode

        :param bool watch: True for setting a watch event as a side effect,
            otherwise False

        :return: Data
        :rtype: bytes

        :raises aiozk.exc.NoNode: Can be raised if path does not exist
        """
        response = await self.get(path, watch=watch)
        return response[0]

    async def set(self, path: str, data: str, version: int) -> protocol.SetDataResponse:
        """
        Set data to znode. Prefer using .set_data than this method unless you
        have to control the concurrency.

        :param str path: Path of znode

        :param data: Data to store at znode
        :type data: str or bytes

        :param int version: Version of znode data to be modified.

        :return: Response stat
        :rtype: aiozk.protocol.stat.Stat

        :raises aiozk.exc.NoNode: Raised if znode does not exist

        :raises aiozk.exc.BadVersion: Raised if version does not match the
            actual version of the data. The update failed.
        """
        path = self.normalize_path(path)
        response = await self.send(protocol.SetDataRequest(path=path, data=data, version=version))
        return response.stat

    async def set_data(self, path, data, force=False):
        """
        Set data to znode without needing to handle version.

        :param str path: Path of znode

        :param data: Data to be stored at znode
        :type data: bytes or str

        :param bool force: True for ignoring data version. False for using
            version from stat cache.

        :raises aiozk.exc.NoNode: Raised if znode does not exist

        :raises aiozk.exc.BadVersion: Raised if force parameter is False and
            only if supplied version from stat cache does not match the actual
            version of znode data.
        """
        path = self.normalize_path(path)

        if not force and path in self.stat_cache:
            version = self.stat_cache[path].version
        else:
            version = -1

        await self.send(protocol.SetDataRequest(path=path, data=data, version=version))

    async def get_children(self, path, watch=False):
        """
        Get all children names. Returned names are only basename and they does
        not include dirname.

        :param str path: Path of znode

        :param bool watch: True for setting a watch event as a side effect
            otherwise False

        :return: Names of children znodes
        :rtype: [str]

        :raises aiozk.exc.NoNode: Raised if znode does not exist
        """
        path = self.normalize_path(path)

        response = await self.send(protocol.GetChildren2Request(path=path, watch=watch))
        return response.children

    async def get_acl(self, path):
        """
        Get list of ACLs associated with the znode

        :param str path: Path of znode

        :return: List of ACLs associated with the znode
        :rtype: [aiozk.ACL]
        """
        path = self.normalize_path(path)

        response = await self.send(protocol.GetACLRequest(path=path))
        return response.acl

    async def set_acl(self, path, acl, force=False):
        """
        Set ACL to the znode.

        :param str path: Path of znode

        :param acl: ACL for the znode
        :type acl: aiozk.ACL

        :param bool force: True for ignoring ACL version of the znode when
            setting ACL to the actual znode. False for using ACL version from
            the stat cache.

        :raises aiozk.exc.NoNode: Raised if znode does not exist

        :raises aiozk.exc.BadVersion: Raised if force parameter is False and
            only if the supplied version from stat cache does not match the
            actual ACL version of the znode.
        """
        path = self.normalize_path(path)

        if not force and path in self.stat_cache:
            version = self.stat_cache[path].version
        else:
            version = -1

        await self.send(protocol.SetACLRequest(path=path, acl=acl, version=version))

    def begin_transaction(self):
        """
        Return Transaction instance which provides methods for read/write
        operations and commit method. This instance is used for
        transaction request.

        :return: Transaction instance which can be used for adding read/write
            operations

        :rtype: aiozk.transaction.Transaction
        """
        return Transaction(self)
