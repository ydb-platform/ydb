"""TreeCache

:Maintainer: Jiangge Zhang <tonyseek@gmail.com>
:Maintainer: Haochuan Guo <guohaochuan@gmail.com>
:Maintainer: Tianwen Zhang <mail2tevin@gmail.com>
:Status: Alpha

A port of the Apache Curator's TreeCache recipe. It builds an in-memory cache
of a subtree in ZooKeeper and keeps it up-to-date.

See also: http://curator.apache.org/curator-recipes/tree-cache.html
"""
from __future__ import absolute_import

import contextlib
import functools
import logging
import operator
import os

from kazoo.exceptions import NoNodeError, KazooException
from kazoo.protocol.paths import _prefix_root
from kazoo.protocol.states import KazooState, EventType


logger = logging.getLogger(__name__)


class TreeCache(object):
    """The cache of a ZooKeeper subtree.

    :param client: A :class:`~kazoo.client.KazooClient` instance.
    :param path: The root path of subtree.
    """

    STATE_LATENT = 0
    STATE_STARTED = 1
    STATE_CLOSED = 2

    _STOP = object()

    def __init__(self, client, path):
        self._client = client
        self._root = TreeNode.make_root(self, path)
        self._state = self.STATE_LATENT
        self._outstanding_ops = 0
        self._is_initialized = False
        self._error_listeners = []
        self._event_listeners = []
        self._task_queue = client.handler.queue_impl()
        self._task_thread = None

    def start(self):
        """Starts the cache.

        The cache is not started automatically. You must call this method.

        After a cache started, all changes of subtree will be synchronized
        from the ZooKeeper server. Events will be fired for those activity.

        Don't forget to call :meth:`close` if a tree was started and you don't
        need it anymore, or you will leak the memory of cached nodes, even if
        you have released all references to the :class:`TreeCache` instance.
        Because there are so many callbacks that have been registered to the
        Kazoo client.

        See also :meth:`~TreeCache.listen`.

        .. note::

            This method is not thread safe.
        """
        if self._state == self.STATE_LATENT:
            self._state = self.STATE_STARTED
        elif self._state == self.STATE_CLOSED:
            raise KazooException('already closed')
        else:
            raise KazooException('already started')

        self._task_thread = self._client.handler.spawn(self._do_background)
        self._client.add_listener(self._session_watcher)
        self._client.ensure_path(self._root._path)

        if self._client.connected:
            # The on_created and other on_* methods must not be invoked outside
            # the background task. This is the key to keep concurrency safe
            # without lock.
            self._in_background(self._root.on_created)

    def close(self):
        """Closes the cache.

        A closed cache was detached from ZooKeeper's changes. And all nodes
        will be invalidated.

        Once a tree cache was closed, it could not be started again. You should
        only close a tree cache while you want to recycle it.

        .. note::

            This method is not thread safe.
        """
        if self._state == self.STATE_STARTED:
            self._state = self.STATE_CLOSED
            self._task_queue.put(self._STOP)
            self._client.remove_listener(self._session_watcher)
            with handle_exception(self._error_listeners):
                # We must invoke on_deleted outside background queue because:
                # 1. The background task has been stopped.
                # 2. The on_deleted on closed tree does not communicate with
                #    ZooKeeper actually.
                self._root.on_deleted()

    def listen(self, listener):
        """Registers a function to listen the cache events.

        The cache events are changes of local data. They are delivered from
        watching notifications in ZooKeeper session.

        This method can be use as a decorator.

        :param listener: A callable object which accepting a
                         :class:`~kazoo.recipe.cache.TreeEvent` instance as
                         its argument.
        """
        self._event_listeners.append(listener)
        return listener

    def listen_fault(self, listener):
        """Registers a function to listen the exceptions.

        It is possible to meet some exceptions during the cache running. You
        could specific handlers for them.

        This method can be use as a decorator.

        :param listener: A callable object which accepting an exception as its
                         argument.
        """
        self._error_listeners.append(listener)
        return listener

    def get_data(self, path, default=None):
        """Gets data of a node from cache.

        :param path: The absolute path string.
        :param default: The default value which will be returned if the node
                        does not exist.
        :raises ValueError: If the path is outside of this subtree.
        :returns: A :class:`~kazoo.recipe.cache.NodeData` instance.
        """
        node = self._find_node(path)
        return default if node is None else node._data

    def get_children(self, path, default=None):
        """Gets node children list from in-memory snapshot.

        :param path: The absolute path string.
        :param default: The default value which will be returned if the node
                        does not exist.
        :raises ValueError: If the path is outside of this subtree.
        :returns: The :class:`frozenset` which including children names.
        """
        node = self._find_node(path)
        return default if node is None else frozenset(node._children)

    def _find_node(self, path):
        if not path.startswith(self._root._path):
            raise ValueError('outside of tree')
        striped_path = path[len(self._root._path):].strip('/')
        splited_path = [p for p in striped_path.split('/') if p]
        current_node = self._root
        for node_name in splited_path:
            if node_name not in current_node._children:
                return
            current_node = current_node._children[node_name]
        return current_node

    def _publish_event(self, event_type, event_data=None):
        event = TreeEvent.make(event_type, event_data)
        if self._state != self.STATE_CLOSED:
            logger.debug('public event: %r', event)
            self._in_background(self._do_publish_event, event)

    def _do_publish_event(self, event):
        for listener in self._event_listeners:
            with handle_exception(self._error_listeners):
                listener(event)

    def _in_background(self, func, *args, **kwargs):
        self._task_queue.put((func, args, kwargs))

    def _do_background(self):
        while True:
            with handle_exception(self._error_listeners):
                cb = self._task_queue.get()
                if cb is self._STOP:
                    break
                func, args, kwargs = cb
                func(*args, **kwargs)

                # release before possible idle
                del cb, func, args, kwargs

    def _session_watcher(self, state):
        if state == KazooState.SUSPENDED:
            self._publish_event(TreeEvent.CONNECTION_SUSPENDED)
        elif state == KazooState.CONNECTED:
            # The session watcher should not be blocked
            self._in_background(self._root.on_reconnected)
            self._publish_event(TreeEvent.CONNECTION_RECONNECTED)
        elif state == KazooState.LOST:
            self._is_initialized = False
            self._publish_event(TreeEvent.CONNECTION_LOST)


class TreeNode(object):
    """The tree node record.

    :param tree: A :class:`~kazoo.recipe.cache.TreeCache` instance.
    :param path: The path of current node.
    :param parent: The parent node reference. ``None`` for root node.
    """

    __slots__ = ('_tree', '_path', '_parent', '_depth', '_children', '_state',
                 '_data')

    STATE_PENDING = 0
    STATE_LIVE = 1
    STATE_DEAD = 2

    def __init__(self, tree, path, parent):
        self._tree = tree
        self._path = path
        self._parent = parent
        self._depth = parent._depth + 1 if parent else 0
        self._children = {}
        self._state = self.STATE_PENDING
        self._data = None

    @classmethod
    def make_root(cls, tree, path):
        return cls(tree, path, None)

    def on_reconnected(self):
        self._refresh()
        for child in self._children.values():
            child.on_reconnected()

    def on_created(self):
        self._refresh()

    def on_deleted(self):
        old_children, self._children = self._children, {}
        old_data, self._data = self._data, None

        for old_child in old_children.values():
            old_child.on_deleted()

        if self._tree._state == self._tree.STATE_CLOSED:
            self._reset_watchers()
            return

        old_state, self._state = self._state, self.STATE_DEAD
        if old_state == self.STATE_LIVE:
            self._publish_event(TreeEvent.NODE_REMOVED, old_data)

        if self._parent is None:
            self._call_client('exists', self._path)  # root node
        else:
            child = self._path[len(self._parent._path) + 1:]
            if self._parent._children.get(child) is self:
                del self._parent._children[child]
                self._reset_watchers()

    def _publish_event(self, *args, **kwargs):
        return self._tree._publish_event(*args, **kwargs)

    def _reset_watchers(self):
        client = self._tree._client
        for _watchers in (client._data_watchers, client._child_watchers):
            _path = _prefix_root(client.chroot, self._path)
            _watcher = _watchers.get(_path, set())
            _watcher.discard(self._process_watch)

    def _refresh(self):
        self._refresh_data()
        self._refresh_children()

    def _refresh_data(self):
        self._call_client('get', self._path)

    def _refresh_children(self):
        # TODO max-depth checking support
        self._call_client('get_children', self._path)

    def _call_client(self, method_name, path):
        assert method_name in ('get', 'get_children', 'exists')
        self._tree._outstanding_ops += 1
        callback = functools.partial(
            self._tree._in_background, self._process_result,
            method_name, path)
        method = getattr(self._tree._client, method_name + '_async')
        method(path, watch=self._process_watch).rawlink(callback)

    def _process_watch(self, watched_event):
        logger.debug('process_watch: %r', watched_event)
        with handle_exception(self._tree._error_listeners):
            if watched_event.type == EventType.CREATED:
                assert self._parent is None, 'unexpected CREATED on non-root'
                self.on_created()
            elif watched_event.type == EventType.DELETED:
                self.on_deleted()
            elif watched_event.type == EventType.CHANGED:
                self._refresh_data()
            elif watched_event.type == EventType.CHILD:
                self._refresh_children()

    def _process_result(self, method_name, path, result):
        logger.debug('process_result: %s %s', method_name, path)
        if method_name == 'exists':
            assert self._parent is None, 'unexpected EXISTS on non-root'
            # The result will be `None` if the node doesn't exist.
            if result.successful() and result.get() is not None:
                if self._state == self.STATE_DEAD:
                    self._state = self.STATE_PENDING
                self.on_created()
        elif method_name == 'get_children':
            if result.successful():
                children = result.get()
                for child in sorted(children):
                    full_path = os.path.join(path, child)
                    if child not in self._children:
                        node = TreeNode(self._tree, full_path, self)
                        self._children[child] = node
                        node.on_created()
            elif isinstance(result.exception, NoNodeError):
                self.on_deleted()
        elif method_name == 'get':
            if result.successful():
                data, stat = result.get()
                old_data, self._data = (
                    self._data, NodeData.make(path, data, stat))
                old_state, self._state = self._state, self.STATE_LIVE
                if old_state == self.STATE_LIVE:
                    if old_data is None or old_data.stat.mzxid != stat.mzxid:
                        self._publish_event(TreeEvent.NODE_UPDATED, self._data)
                else:
                    self._publish_event(TreeEvent.NODE_ADDED, self._data)
            elif isinstance(result.exception, NoNodeError):
                self.on_deleted()
        else:  # pragma: no cover
            logger.warning('unknown operation %s', method_name)
            self._tree._outstanding_ops -= 1
            return

        self._tree._outstanding_ops -= 1
        if self._tree._outstanding_ops == 0 and not self._tree._is_initialized:
            self._tree._is_initialized = True
            self._publish_event(TreeEvent.INITIALIZED)


class TreeEvent(tuple):
    """The immutable event tuple of cache."""

    NODE_ADDED = 0
    NODE_UPDATED = 1
    NODE_REMOVED = 2
    CONNECTION_SUSPENDED = 3
    CONNECTION_RECONNECTED = 4
    CONNECTION_LOST = 5
    INITIALIZED = 6

    #: An enumerate integer to indicate event type.
    event_type = property(operator.itemgetter(0))

    #: A :class:`~kazoo.recipe.cache.NodeData` instance.
    event_data = property(operator.itemgetter(1))

    @classmethod
    def make(cls, event_type, event_data):
        """Creates a new TreeEvent tuple.

        :returns: A :class:`~kazoo.recipe.cache.TreeEvent` instance.
        """
        assert event_type in (
            cls.NODE_ADDED, cls.NODE_UPDATED, cls.NODE_REMOVED,
            cls.CONNECTION_SUSPENDED, cls.CONNECTION_RECONNECTED,
            cls.CONNECTION_LOST, cls.INITIALIZED)
        return cls((event_type, event_data))


class NodeData(tuple):
    """The immutable node data tuple of cache."""

    #: The absolute path string of current node.
    path = property(operator.itemgetter(0))

    #: The bytes data of current node.
    data = property(operator.itemgetter(1))

    #: The stat information of current node.
    stat = property(operator.itemgetter(2))

    @classmethod
    def make(cls, path, data, stat):
        """Creates a new NodeData tuple.

        :returns: A :class:`~kazoo.recipe.cache.NodeData` instance.
        """
        return cls((path, data, stat))


@contextlib.contextmanager
def handle_exception(listeners):
    try:
        yield
    except Exception as e:
        logger.debug('processing error: %r', e)
        if listeners:
            for listener in listeners:
                try:
                    listener(e)
                except BaseException:  # pragma: no cover
                    logger.exception('Exception handling exception')  # oops
        else:
            logger.exception('No listener to process %r', e)
