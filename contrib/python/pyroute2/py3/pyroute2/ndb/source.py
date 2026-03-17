'''

Local RTNL
----------

Local RTNL source is a simple `IPRoute` instance. By default NDB
starts with one local RTNL source names `localhost`::

    >>> ndb = NDB()
    >>> ndb.sources.summary().format("json")
    [
        {
            "name": "localhost",
            "spec": "{'target': 'localhost', 'nlm_generator': 1}",
            "state": "running"
        }
    ]
    >>> ndb.sources['localhost']
    [running] <IPRoute {'target: 'localhost', 'nlm_generator': 1}>

The `localhost` RTNL source starts an additional async cache thread.
The `nlm_generator` option means that instead of collections the
`IPRoute` object returns generators, so `IPRoute` responses will not
consume memory regardless of the RTNL objects number::

    >>> ndb.sources['localhost'].nl.link('dump')
    <generator object RTNL_API.filter_messages at 0x7f61a99a34a0>

See also: :ref:`iproute`

Network namespaces
------------------

There are two ways to connect additional sources to an NDB instance.
One is to specify sources when creating an NDB object::

    ndb = NDB(sources=[{'target': 'localhost'}, {'netns': 'test01'}])

Another way is to call `ndb.sources.add()` method::

    ndb.sources.add(netns='test01')

This syntax: `{target': 'localhost'}` and `{'netns': 'test01'}` is the
short form. The full form would be::

    {'target': 'localhost', # the label for the DB
     'kind': 'local',       # use IPRoute class to start the source
     'nlm_generator': 1}    #

    {'target': 'test01',    # the label
     'kind': 'netns',       # use NetNS class
     'netns': 'test01'}     #

See also: :ref:`netns`
'''

import errno
import socket
import struct
import threading
import time

from pyroute2 import netns
from pyroute2.common import basestring
from pyroute2.iproute import AsyncIPRoute
from pyroute2.netlink.exceptions import NetlinkError
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg

from .events import ShutdownException, State
from .messages import cmsg_event
from .objects import RTNL_Object

SOURCE_FAIL_PAUSE = 1
SOURCE_MAX_ERRORS = 3


class Source(dict):
    '''
    The RNTL source. The source that is used to init the object
    must comply to IPRoute API, must support the async_cache. If
    the source starts additional threads, they must be joined
    in the source.close()
    '''

    table_alias = 'src'
    dump_header = None
    summary_header = None
    view = None
    table = 'sources'

    def __init__(self, ndb, **spec):
        self.th = None
        self.task = None
        self.nl = None
        self.ndb = ndb
        self.evq = self.ndb.task_manager.event_queue
        # the target id -- just in case
        self.target = spec['target']
        self.kind = spec.pop('kind', 'local')
        self.max_errors = spec.pop('max_errors', SOURCE_MAX_ERRORS)
        self.event = spec.pop('event')
        # RTNL API
        self.nl_kwarg = spec
        self.nl_kwarg['nlm_echo'] = True
        self.errors_counter = 0
        self.exception = None
        self.shutdown = threading.Event()
        self.started = threading.Event()
        self.lock = threading.RLock()
        self.shutdown_lock = threading.RLock()
        self.started.clear()
        self.log = ndb.log.channel('sources.%s' % self.target)
        self.state = State(log=self.log, wait_list=['running'])
        self.state.set('init')
        self.ndb.schema.add_nl_source(self.target, self.kind, spec)
        self.load_sql()

    @classmethod
    def _count(cls, view):
        return view.ndb.schema.fetchone(f'SELECT count(*) FROM {view.table}')

    @property
    def must_restart(self):
        if self.max_errors < 0 or self.errors_counter <= self.max_errors:
            return True
        return False

    @property
    def bind_arguments(self):
        return dict(
            filter(
                lambda x: x[1] is not None,
                (
                    ('async_cache', True),
                    ('clone_socket', True),
                    ('groups', self.nl_kwarg.get('groups')),
                ),
            )
        )

    async def set_ready(self):
        try:
            if self.event is not None:
                await self.evq.put(cmsg_event(self.target, self.event))
        except ShutdownException:
            self.state.set('stop')
            return False
        return True

    @classmethod
    def defaults(cls, spec):
        ret = dict(spec)
        defaults = {}
        if 'hostname' in spec:
            defaults['kind'] = 'remote'
            defaults['protocol'] = 'ssh'
            defaults['target'] = spec['hostname']
        if 'netns' in spec:
            defaults['kind'] = 'netns'
            defaults['target'] = spec['netns']
            ret['netns'] = netns._get_netnspath(spec['netns'])
        for key in defaults:
            if key not in ret:
                ret[key] = defaults[key]
        return ret

    def __repr__(self):
        return '[%s] <%s>' % (self.state.get(), self.nl_kwarg)

    @classmethod
    def nla2name(cls, name):
        return name

    @classmethod
    def name2nla(cls, name):
        return name

    @classmethod
    def summary(cls, view):
        yield ('state', 'name', 'spec')
        for key in view.keys():
            yield (view[key].state.get(), key, '%s' % (view[key].nl_kwarg,))

    @classmethod
    def dump(cls, view):
        return cls.summary(view)

    @classmethod
    def compare_record(self, left, right):
        # specific compare
        if isinstance(right, basestring):
            return right == left['name']

    async def api(self, name, *argv, **kwarg):
        for _ in range(100):  # FIXME make a constant
            with self.lock:
                try:
                    self.log.debug(f'source api run {name} {argv} {kwarg}')
                    result = await getattr(self.nl, name)(*argv, **kwarg)
                    if isinstance(result, list):
                        for msg in result:
                            await self.evq.put(msg)
                    return result
                except (
                    NetlinkError,
                    AttributeError,
                    ValueError,
                    KeyError,
                    TypeError,
                    socket.error,
                    struct.error,
                ):
                    raise
                except Exception as e:
                    # probably the source is restarting
                    self.errors_counter += 1
                    self.log.debug(f'source api error: <{e}>')
                    time.sleep(1)
        raise RuntimeError('api call failed')

    async def fake_zero_if(self):
        url = 'https://github.com/svinota/pyroute2/issues/737'
        zero_if = ifinfmsg()
        zero_if['index'] = 0
        zero_if['state'] = 'up'
        zero_if['flags'] = 1
        zero_if['family'] = 0
        zero_if['header']['flags'] = 2
        zero_if['header']['type'] = 16
        zero_if['header']['target'] = self.target
        zero_if['event'] = 'RTM_NEWLINK'
        zero_if['attrs'] = [
            ('IFLA_IFNAME', url),
            ('IFLA_ADDRESS', '00:00:00:00:00:00'),
        ]
        zero_if.encode()
        await self.evq.put(zero_if)

    async def receiver(self):
        #
        # The source thread routine -- get events from the
        # channel and forward them into the common event queue
        #
        # The routine exists on an event with error code == 104
        #
        if self.nl is not None:
            try:
                self.nl.close(code=0)
            except Exception as e:
                self.log.warning('source restart: %s' % e)
        try:
            self.state.set('connecting')
            spec = {}
            spec.update(self.nl_kwarg)
            self.nl = AsyncIPRoute(**spec)
            self.state.set('loading')
            #
            await self.nl.setup_endpoint()
            await self.nl.bind(**self.bind_arguments)
            self.log.debug(f'source fd {self.nl.fileno()}')
            #
            # Initial load -- enqueue the data
            #
            self.ndb.schema.flush(self.target)
            if self.kind in ('local', 'netns', 'remote'):
                await self.fake_zero_if()
            async for x in await self.nl.dump():
                await self.evq.put(x)
            self.state.set('running')
        finally:
            await self.set_ready()

        while self.state.get() not in ('stop', 'restart'):
            async for msg in self.nl.get():
                error = msg['header']['error']
                if error:
                    raise error
                await self.evq.put(msg)

    def close(self, code=errno.ECONNRESET, sync=True):
        with self.shutdown_lock:
            if self.shutdown.is_set():
                self.log.debug('already stopped')
                return
            self.log.debug('source shutdown')
            self.shutdown.set()
            if self.nl is not None:
                try:
                    self.nl.close(code=code)
                except Exception as e:
                    self.log.error('source close: %s' % e)
        if sync:
            if self.th is not None:
                self.th.join()
                self.th = None
            else:
                self.log.debug('receiver thread missing')

    def restart(self, reason='unknown'):
        with self.lock:
            with self.shutdown_lock:
                self.log.debug('restarting the source, reason <%s>' % (reason))
                self.started.clear()
                try:
                    self.close()
                    if self.th:
                        self.th.join()
                    self.shutdown.clear()
                    self.start()
                finally:
                    pass
        self.started.wait()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def load_sql(self):
        #
        spec = self.ndb.schema.fetchone(
            'SELECT * FROM sources WHERE f_target = ?', (self.target,)
        )
        self['target'], self['kind'] = spec
        for spec in self.ndb.schema.fetch(
            'SELECT * FROM sources_options WHERE f_target = ?', (self.target,)
        ):
            f_target, f_name, f_type, f_value = spec
            self[f_name] = int(f_value) if f_type == 'int' else f_value


class SyncSource(RTNL_Object):

    @property
    def nl(self):
        return self.asyncore.nl

    def api(self, name, *argv, **kwarg):
        return self._main_sync_call(self.asyncore.api, name, *argv, **kwarg)

    def set(self, key, value):
        if key == 'state':
            self.asyncore.ndb.task_manager.task_map[
                self.asyncore.task_id
            ].state.set(value)
            return
        raise RuntimeError('unknown property')

    def restart(self, reason='no reason'):
        self.asyncore.event.clear()
        self.close(next_state='running')
        self._main_async_call(self.asyncore.event.wait)

    def close(self, code=errno.ECONNRESET, sync=None, next_state='stopped'):
        self.set('state', next_state)
        self._main_sync_call(
            self.asyncore.ndb.schema.flush, self.asyncore.target
        )
        self._main_sync_call(self.asyncore.nl.close, code)
