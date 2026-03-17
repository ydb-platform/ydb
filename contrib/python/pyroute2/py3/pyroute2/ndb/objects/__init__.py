'''
General structure
=================

The NDB objects are dictionary-like structures that represent network
objects -- interfaces, routes, addresses etc. They support the common
dict API, like item getting, setting, iteration through key and values.
In addition to that, NDB object add specific calls, see the API section
below.

Most of the NDB object types store all attributes in a flat one level
dictionary. Some, like multihop routes, implement nested structures.
In addition to that, some objects like `Interface` provide views on the
DB that list only related objects -- addresses, routes and neighbours.
More on these topic see in the corresponding sections.

NDB objects and RTNL API
========================

The dictionary fields represent RTNL messages fields and NLA names,
and the objects are used as argument dictionaries to normal `IPRoute`
methods like `link()` or `route()`. Thus everything described for
the `IPRoute` methods is valid here as well.

See also: :ref:`iproute`


.. testsetup::

    from pyroute2 import IPRoute, NDB
    from pyroute2 import config

    config.mock_netlink = True

.. testcode::

    # create a vlan interface with IPRoute
    eth0 = 2
    with IPRoute() as ipr:
        ipr.link("add",
                 ifname="vlan1108",
                 kind="vlan",
                 link=eth0,
                 vlan_id=1108)

    # same with NDB:
    with NDB(log="stderr") as ndb:
        vlan = ndb.interfaces.create(
            ifname="vlan1108",
            kind="vlan",
            link="eth0",
            vlan_id=1108,
        )
        vlan.commit()

Slightly simplifying, if a network object doesn't exist, NDB will run
an RTNL method with "add" argument, if exists -- "set", and to remove
an object NDB will call the method with "del" argument.

API
===
'''

import asyncio
import collections
import errno
import json
import threading
import time
import traceback
import weakref
from enum import IntFlag
from functools import partial
from typing import Awaitable, Callable, Union

from pyroute2.netlink.exceptions import NetlinkError
from pyroute2.requests.main import RequestProcessor

from ..events import InvalidateHandlerException, State
from ..report import Record
from ..sync_api import SyncBase

RSLV_IGNORE = 0
RSLV_RAISE = 1
RSLV_NONE = 2
RSLV_DELETE = 3


# FIXME:3.9: TypeAlias
# FIXME 3.9: Union
Req = dict[str, Union[str, int]]


async def fallback_add(self, idx_req, req):
    # ignore all set/get for objects with incomplete idx_req
    if set(idx_req.keys()) != set(self.kspec):
        self.log.debug('ignore incomplete idx_req in the fallback')
        return
    # try to set the object
    self.sources[self['target']].api(self.api, 'set', **req)
    # try to get the object
    self.sources[self['target']].api(self.api, 'get', **idx_req)
    # reload the collected data
    self.load_sql()


class ObjectFlags(IntFlag):
    UNSPEC = 0x0
    SNAPSHOT = 0x1


class AsyncObject(dict):
    '''
    The common base class for NDB objects -- interfaces, routes, rules
    addresses etc. Implements common logic for all the classes, like
    item setting, commit/rollback, RTNL event filters, loading values
    from the DB backend etc.
    '''

    view = None  # (optional) view to load values for the summary etc.
    utable = None  # table to send updates to

    resolve_fields = []
    key_extra_fields = []
    hidden_fields = []
    fields_cmp = {}
    fields_load_transform = {}
    field_filter = object
    rollback_chain = []

    fallback_for = None
    schema = None
    event_map = None
    state = None
    log = None
    errors = None
    msg_class = None
    reverse_update = None
    _table = None
    _apply_script = None
    _apply_script_snapshots = []
    _key = None
    _replace = None
    _replace_on_key_change = False
    _init_complete = False

    # 8<------------------------------------------------------------
    #
    # Documented public properties section
    #
    @property
    def table(self):
        '''
        Main reference table for the object. The SQL schema of this
        table is used to build the object key and to verify fields.

        Read-write property.
        '''
        return self._table

    @table.setter
    def table(self, value):
        self._table = value

    @property
    def etable(self):
        '''
        Effective table where the object actually fetches the data from.
        It is not always equal `self.table`, e.g. snapshot objects fetch
        the data from snapshot tables.

        Read-only property.
        '''
        if self.ctxid:
            return '%s_%s' % (self.table, self.ctxid)
        else:
            return self.table

    @property
    def key(self):
        '''
        Key of the object, used to build SQL requests to fetch
        the data from the DB.

        Read-write property.
        '''
        nkey = self._key or {}
        ret = collections.OrderedDict()
        for name in self.kspec:
            kname = self.iclass.nla2name(name)
            if kname in self:
                value = self[kname]
                if value is None and name in nkey:
                    value = nkey[name]
                if isinstance(value, (list, tuple, dict)):
                    value = json.dumps(value)
                ret[name] = value
        if len(ret) < len(self.kspec):
            for name in self.key_extra_fields:
                kname = self.iclass.nla2name(name)
                if self.get(kname):
                    ret[name] = self[kname]
        return ret

    @key.setter
    def key(self, k):
        if not isinstance(k, dict):
            return
        for key, value in k.items():
            if value is not None:
                dict.__setitem__(self, self.iclass.nla2name(key), value)

    #
    # 8<------------------------------------------------------------
    #
    @classmethod
    def _count(cls, view):
        return view.ndb.schema.fetchone('SELECT count(*) FROM %s' % view.table)

    @classmethod
    def _dump_where(cls, view):
        return '', []

    @classmethod
    def _sdump(cls, view, names, fnames):
        req = '''
              SELECT %s FROM %s AS main
              ''' % (
            fnames,
            cls.table,
        )
        yield names
        where, values = cls._dump_where(view)
        for record in view.ndb.schema.fetch(req + where, values):
            yield record

    @classmethod
    def summary(cls, view):
        return cls._sdump(
            view,
            view.ndb.schema.compiled[cls.table]['norm_idx'],
            view.ndb.schema.compiled[cls.table]['knames'],
        )

    @classmethod
    def dump(cls, view):
        return cls._sdump(
            view,
            view.ndb.schema.compiled[cls.table]['norm_names'],
            view.ndb.schema.compiled[cls.table]['fnames'],
        )

    @classmethod
    def spec_normalize(cls, spec):
        return spec

    @staticmethod
    def key_load_context(key, context):
        return key

    def __init__(
        self,
        view,
        key,
        iclass,
        ctxid=None,
        load=True,
        master=None,
        check=True,
        flags=ObjectFlags.UNSPEC,
    ):
        self.view = view
        self.ndb = view.ndb
        self.sources = view.ndb.sources.asyncore
        self.master = master
        self.ctxid = ctxid
        self.schema = view.ndb.schema
        self.task_manager = view.ndb.task_manager
        self.changed = set()
        self.iclass = iclass
        self.utable = self.utable or self.table
        self.errors = []
        self.flags = flags
        self.atime = time.time()
        self.log = self.ndb.log.channel('rtnl_object')
        self.log.debug('init')
        self.state = State()
        self.state.set('invalid')
        self.snapshot_deps = []
        self.load_event = asyncio.Event()
        self.evq = self.ndb.task_manager.event_queue
        self.load_event.set()
        self.load_debug = False
        self.lock = threading.Lock()
        self.object_data = RequestProcessor(context=weakref.proxy(self))
        self.object_data.add_filter(self.field_filter())
        self.kspec = self.schema.compiled[self.table]['idx']
        self.knorm = self.schema.compiled[self.table]['norm_idx']
        self.spec = self.schema.compiled[self.table]['all_names']
        self.names = self.schema.compiled[self.table]['norm_names']
        self.lookup_fallbacks = self.schema.compiled[self.table][
            'lookup_fallbacks'
        ]
        self.names_count = [self.names.count(x) for x in self.names]
        self.last_save = None
        if self.event_map is None:
            self.event_map = {}
        self._apply_script = []
        self.fallback_for = {
            'add': {errno.EEXIST: fallback_add, errno.EAGAIN: None},
            'set': {errno.ENODEV: None},
            'del': {
                errno.ENODEV: None,  # interfaces
                errno.ENOENT: None,  # rules
                errno.ESRCH: None,  # routes
                errno.EADDRNOTAVAIL: None,  # addresses
            },
        }
        if isinstance(key, dict):
            self.chain = key.pop('ndb_chain', None)
            create = key.pop('create', False)
        else:
            self.chain = None
            create = False
        exists = self.exists(key)
        ckey = self.complete_key(key)
        if create:
            if check & exists:
                raise KeyError('object exists')
            for name in key:
                self[self.iclass.nla2name(name)] = key[name]
            # FIXME -- merge with complete_key()
            if 'target' not in self:
                self.load_value('target', self.view.default_target)
        else:
            if not exists:
                raise KeyError('object does not exists')
            self.key = ckey
            if load:
                if ctxid is None:
                    self.load_sql()
                else:
                    self.load_sql(table=self.table)
        self._init_complete = True

    @classmethod
    def new_spec(cls, spec, context=None, localhost=None):
        if isinstance(spec, Record):
            spec = spec._as_dict()
        spec = cls.spec_normalize(spec)
        rp = RequestProcessor(context=spec)
        rp.add_filter(cls.field_filter())
        rp.update(spec)
        if isinstance(context, dict):
            rp.update(context)
        if 'target' not in rp and localhost is not None:
            rp['target'] = localhost
        return rp

    @staticmethod
    def resolve(view, spec, fields, policy=RSLV_IGNORE):
        '''
        Resolve specific fields e.g. convert port ifname into index.
        '''
        for field in fields:
            reference = spec.get(field)
            try:
                if isinstance(reference, dict) and 'index' in reference:
                    spec[field] = reference['index']
                elif reference is not None and not isinstance(reference, int):
                    spec[field] = view[reference]['index']
            except (KeyError, TypeError):
                if policy == RSLV_RAISE:
                    raise
                elif policy == RSLV_NONE:
                    spec[field] = None
                elif policy == RSLV_DELETE:
                    del spec[field]
                elif policy == RSLV_IGNORE:
                    pass
                else:
                    raise TypeError('unknown rslv policy')

    def mark_tflags(self, mark):
        pass

    def keys(self):
        return filter(lambda x: x not in self.hidden_fields, dict.keys(self))

    def items(self):
        return filter(
            lambda x: x[0] not in self.hidden_fields, dict.items(self)
        )

    @property
    def context(self):
        return {'target': self.get('target', self.ndb.config.localhost)}

    @classmethod
    def nla2name(self, name):
        return self.msg_class.nla2name(name)

    @classmethod
    def compare_record(self, left, right):
        pass

    @classmethod
    def name2nla(self, name):
        return self.msg_class.name2nla(name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.commit()

    def __hash__(self):
        return id(self)

    def __getitem__(self, key):
        return dict.__getitem__(self, key)

    def __setitem__(self, key, value):
        for nkey, nvalue in self.object_data.filter(key, value).items():
            if self.get(nkey) == nvalue:
                continue
            if self.state == 'system' and nkey in self.knorm:
                if self._replace_on_key_change:
                    self.log.debug(
                        f'prepare replace {nkey} = {nvalue} in {self.key}'
                    )
                    self._replace = type(self)(self.view, self.key)
                    self.state.set('replace')
                else:
                    raise ValueError(
                        'attempt to change a key field (%s)' % nkey
                    )
            if nkey in ('net_ns_fd', 'net_ns_pid'):
                self.state.set('setns')
            if nvalue != self.get(nkey, None):
                if nkey != 'target':
                    self.changed.add(nkey)
                dict.__setitem__(self, nkey, nvalue)

    def fields(self, *argv):
        # TODO: deprecate and move to show()
        Fields = collections.namedtuple('Fields', argv)
        return Fields(*[self[key] for key in argv])

    def key_repr(self):
        return repr(self.key)

    def create(self, **spec):
        '''
        Create an RTNL object of the same type, and add it to the
        commit chain. The spec format depends on the object.

        The method allows to chain creation of multiple objects sharing
        the same context.

        .. code-block:: python

            (
                ndb.interfaces['eth0']                     # 1.
                .set(state="up")                           # 2.
                .ipaddr                                    # 3.
                .create(address='10.0.0.1', prefixlen=24)  # 4. <- create()
                .create(address='10.0.0.2', prefixlen=24)  # 5. <- create()
                .commit()                                  # 6.
            )

        Here:

        1. returns an interface object `eth0`
        2. sets `state="up"` and returns the object itself
        3. returns an address view, that uses `eth0` as the context
        4. creates an IP address, the interface lookup is done via context
        5. creates another IP address -- same type, same context
        6. commits the changes in the order: (interface `state="up"`;
           address `10.0.0.1/24`; address `10.0.0.2/24`)
        '''
        spec['create'] = True
        spec['ndb_chain'] = self
        return self.view[spec]

    def show(self, fmt=None):
        '''
        Return the object in a specified format. The format may be
        specified with the keyword argument `format` or in the
        `ndb.config.show_format`.

        TODO: document different formats
        '''
        fmt = fmt or self.view.ndb.config.show_format
        if fmt == 'native':
            return dict(self)
        else:
            out = collections.OrderedDict()
            for key in sorted(self):
                out[key] = self[key]
            return '%s\n' % json.dumps(out, indent=4, separators=(',', ': '))

    def set(self, *argv, **kwarg):
        '''
        Call formats:

        * `set(key, value)`
        * `set(key=value)`
        * `set(key1=value1, key2=value2)`

        .. code-block:: python

            with ndb.interfaces["eth0"] as eth0:
                eth0.set(
                    mtu=1200,
                    state='up',
                    address='00:11:22:33:44:55',
                )
        '''
        if argv:
            self[argv[0]] = argv[1]
        elif kwarg:
            for key, value in kwarg.items():
                self[key] = value
        return self

    def register(self):
        #
        # Construct a weakref handler for events.
        #
        # If the referent doesn't exist, raise the
        # exception to remove the handler from the
        # chain.
        #
        def wr_handler(wr, fname, *argv):
            try:
                return getattr(wr(), fname)(*argv)
            except Exception:
                # check if the weakref became invalid
                if wr() is None:
                    raise InvalidateHandlerException()
                raise

        wr = weakref.ref(self)
        for event, fname in self.event_map.items():
            #
            # Do not trust the implicit scope and pass the
            # weakref explicitly via partial
            #
            (
                self.ndb.task_manager.register_handler(
                    event, partial(wr_handler, wr, fname)
                )
            )

    async def snapshot(self, ctxid=None):
        '''
        Create and return a snapshot of the object. The method creates
        corresponding SQL tables for the object itself and for detected
        dependencies.

        The snapshot tables will be removed as soon as the snapshot gets
        collected by the GC.
        '''
        ctxid = ctxid or self.ctxid or id(self)
        if self._replace is None:
            key = self.key
        else:
            key = self._replace.key
        snp = type(self)(
            self.view, key, ctxid=ctxid, flags=ObjectFlags.SNAPSHOT
        )
        snp.register()
        self.ndb.schema.save_deps(ctxid, weakref.ref(snp), self.iclass)
        snp.changed = set(self.changed)
        return snp

    def complete_key(self, key):
        '''
        Try to complete the object key based on the provided fields.
        E.g.::

            >>> ndb.interfaces['eth0'].complete_key({"ifname": "eth0"})
            {'ifname': 'eth0',
             'index': 2,
             'target': u'localhost',
             'tflags': 0}

        It is an internal method and is not supposed to be used externally.
        '''
        self.log.debug(f'complete key {key} from table {self.etable}')
        fetch = []
        if isinstance(key, Record):
            key = key._as_dict()
        else:
            key = dict(key)

        self.resolve(
            view=self.view,
            spec=key,
            fields=self.resolve_fields,
            policy=RSLV_DELETE,
        )

        for name in self.kspec:
            if name not in key:
                fetch.append(f'f_{name}')

        if fetch:
            keys = []
            values = []
            for name, value in key.items():
                nla_name = self.iclass.name2nla(name)
                if nla_name in self.spec:
                    name = nla_name
                if value is not None and name in self.spec:
                    keys.append(f'f_{name} = ?')
                    values.append(value)
            spec = self.ndb.schema.fetchone(
                'SELECT %s FROM %s WHERE %s'
                % (' , '.join(fetch), self.etable, ' AND '.join(keys)),
                values,
            )
            if spec is None:
                self.log.debug('got none')
                return None
            for name, value in zip(fetch, spec):
                key[name[2:]] = value

        self.log.debug(f'got {key}')
        return key

    def exists(self, key):
        '''
        Check if the object exists in the DB
        '''
        return self.view.exists(key)

    async def rollback(self, snapshot=None):
        '''
        Try to rollback the object state using the snapshot provided as
        an argument or using `self.last_save`.
        '''
        if self._replace is not None:
            self.log.debug(
                'rollback replace: %s :: %s' % (self.key, self._replace.key)
            )
            new_replace = type(self)(self.view, self.key)
            new_replace.state.set('remove')
            self.state.set('replace')
            self.update(self._replace)
            self._replace = new_replace
        self.log.debug('rollback: %s' % str(self.state.events))
        snapshot = snapshot or self.last_save
        if snapshot == -1:
            self.remove()
            await self.apply()
            return
        else:
            snapshot.state.set(self.state.get())
            snapshot.rollback_chain = self._apply_script_snapshots
            snapshot.flags &= ~ObjectFlags.SNAPSHOT
            await snapshot.apply(rollback=True)
            for link, snp in snapshot.snapshot_deps:
                await link.rollback(snapshot=snp)
            return self

    def clear(self):
        pass

    @property
    def clean(self):
        return (
            self.state == 'system'
            and not self.changed
            and not self._apply_script
        )

    async def commit(self):
        '''
        Commit the pending changes. If an exception is raised during
        `commit()`, automatically `rollback()` to the latest saved snapshot.
        '''
        if self.clean:
            return self

        if self.chain:
            await self.chain.commit()
        self.log.debug('commit: %s' % str(self.state.events))
        # Is it a new object?
        if self.state == 'invalid':
            # Save values, try to apply
            save = dict(self)
            self.last_save = -1
            try:
                return await self.apply(mode='commit')
            except Exception as e_i:
                # Save the debug info
                e_i.trace = traceback.format_exc()
                # ACHTUNG! The routine doesn't clean up the system
                #
                # Drop all the values and rollback to the initial state
                for key in tuple(self.keys()):
                    del self[key]
                for key in save:
                    dict.__setitem__(self, key, save[key])
                raise e_i

        # Continue with an existing object

        # The snapshot tables in the DB will be dropped as soon as the GC
        # collects the object. But in the case of an exception the `snp`
        # variable will be saved in the traceback, so the tables will be
        # available to debug. If the traceback will be saved somewhere then
        # the tables will never be dropped by the GC, so you can do it
        # manually by `ndb.schema.purge_snapshots()` -- to invalidate
        # all the snapshots and to drop the associated tables.

        self.last_save = await self.snapshot()
        # Apply the changes
        try:
            await self.apply(mode='commit')
        except Exception as e_c:
            # Rollback in the case of any error
            try:
                await self.rollback()
            except Exception as e_r:
                e_c.chain = [e_r]
                if hasattr(e_r, 'chain'):
                    e_c.chain.extend(e_r.chain)
                e_r.chain = None
            raise
        finally:
            if self.last_save is not None:
                (self.last_save.state.set(self.state.get()))
        if self._replace is not None:
            self._replace = None
        return self

    def remove(self):
        '''
        Set the desired state to `remove`, so the next `apply()` call will
        delete the object from the system.
        '''
        with self.lock:
            self.state.set('remove')
            return self

    def check(self):
        state_map = (
            ('invalid', 'system'),
            ('remove', 'invalid'),
            ('setns', 'invalid'),
            ('setns', 'system'),
            ('replace', 'system'),
        )

        self.load_sql()
        self.log.debug('check: %s' % str(self.state.events))

        if self.state.transition() not in state_map:
            self.log.debug('check state: False')
            return False

        if self.changed:
            self.log.debug('check changed: %s' % (self.changed))
            return False

        self.log.debug('check: True')
        return True

    def make_req(self, prime):
        req = dict(prime)
        for key in self.changed:
            req[key] = self[key]
        return req

    def make_idx_req(self, prime):
        return prime

    def get_count(self):
        conditions = []
        values = []
        for name in self.kspec:
            conditions.append(f'f_{name} = ?')
            values.append(self.get(self.iclass.nla2name(name), None))
        return (
            self.ndb.schema.fetchone(
                '''
                          SELECT count(*) FROM %s WHERE %s
                          '''
                % (self.table, ' AND '.join(conditions)),
                values,
            )
        )[0]

    async def hook_apply(self, method, **spec):
        pass

    async def save_context(self):
        if self.state == 'invalid':
            self.last_save = -1
        else:
            self.last_save = await self.snapshot()
        return self

    async def apply(self, rollback=False, req_filter=None, mode='apply'):
        '''
        Apply the pending changes. If an exception is raised during
        `apply()`, no `rollback()` is called. No automatic snapshots
        are made.

        In order to properly revert the changes, you have to run::

            await obj.save_context()
            try:
                await obj.apply()
            except Exception:
                await obj.rollback()
        '''

        # Resolve the fields
        self.resolve(
            view=self.view,
            spec=self,
            fields=self.resolve_fields,
            policy=RSLV_RAISE,
        )

        self.log.debug('events log: %s' % str(self.state.events))
        self.log.debug('run apply')
        self.load_event.clear()
        self._apply_script_snapshots = []

        # Load the current state
        try:
            self.schema.commit()
        except Exception:
            pass
        self.load_sql(set_state=False)
        if self.state == 'system' and self.get_count() == 0:
            state = self.state.set('invalid')
        else:
            state = self.state.get()

        # Create the request.
        prime = {
            x: self[x]
            for x in self.schema.compiled[self.table]['norm_idx']
            if self.get(x) is not None
        }
        req = self.make_req(prime)
        idx_req = self.make_idx_req(prime)
        self.log.debug('apply req: %s' % str(req))
        self.log.debug('apply idx_req: %s' % str(idx_req))
        method = None
        #
        if state in ('invalid', 'replace'):
            for k, v in tuple(self.items()):
                if k not in req and v is not None:
                    req[k] = v
            if self.master is not None:
                req = self.new_spec(
                    req, self.master.context, self.ndb.config.localhost
                )

            method = 'add'
        elif state == 'system':
            method = 'set'
        elif state == 'setns':
            method = 'set'
        elif state == 'remove':
            method = 'del'
            req = idx_req
        else:
            raise Exception('state transition not supported')
        self.log.debug(f'apply transition from: {state}')
        self.log.debug(f'apply method: {method}')

        if req_filter is not None:
            req = req_filter(req)

        first_call_success = False
        for itn in range(10):
            try:
                self.log.debug('API call %s (%s)' % (method, req))
                await self.sources[self['target']].api(self.api, method, **req)
                first_call_success = True
                await self.hook_apply(method, **req)
            except NetlinkError as e:
                self.log.debug('error: %s' % e)
                if not first_call_success:
                    self.log.debug('error on the first API call, escalate')
                    raise
                ##
                #
                # FIXME: performance penalty
                # required now only in some NDA corner cases
                # must be moved to objects.neighbour
                #
                #
                ##
                if e.code in self.fallback_for[method]:
                    self.log.debug('ignore error %s for %s' % (e.code, self))
                    if self.fallback_for[method][e.code] is not None:
                        self.log.debug(
                            'run fallback %s (%s)'
                            % (self.fallback_for[method][e.code], req)
                        )
                        try:
                            if isinstance(
                                self.fallback_for[method][e.code], str
                            ):
                                await self.sources[self['target']].api(
                                    self.api,
                                    self.fallback_for[method][e.code],
                                    **req,
                                )
                            else:
                                await self.fallback_for[method][e.code](
                                    self, idx_req, req
                                )
                        except NetlinkError:
                            pass
                else:
                    raise e

            nq = self.schema.stats.get(self['target'])
            if nq is not None:
                nqsize = nq.qsize
            else:
                nqsize = 0
            self.log.debug(
                f'stats: apply {method} '
                f'{{ objid {id(self)}, nqsize {nqsize} }}'
            )
            if self.check():
                self.log.debug('checked')
                break
            self.log.debug('check failed')
            try:
                await asyncio.wait_for(self.load_event.wait(), 1)
            except asyncio.TimeoutError:
                pass
            self.load_event.clear()
        else:
            self.log.debug('stats: %s apply %s fail' % (id(self), method))
            if not await self.use_db_resync(lambda x: x, self.check):
                self._apply_script = []
                raise Exception('could not apply the changes')

        self.log.debug('stats: %s pass' % (id(self)))
        #
        if state == 'replace':
            self._replace.remove()
            await self._replace.apply()
        #
        if rollback:
            #
            # Iterate all the snapshot tables and collect the diff
            for cls in self.view.classes.values():
                if issubclass(type(self), cls) or issubclass(cls, type(self)):
                    continue
                table = cls.table
                # comprare the tables
                diff = self.ndb.schema.fetch(
                    '''
                    SELECT * FROM %s_%s
                      EXCEPT
                    SELECT * FROM %s
                    '''
                    % (table, self.ctxid, table)
                )
                for record in diff:
                    record = dict(
                        zip((self.schema.compiled[table]['all_names']), record)
                    )
                    key = dict(
                        [
                            x
                            for x in record.items()
                            if x[0] in self.schema.compiled[table]['idx']
                        ]
                    )
                    key['create'] = True
                    try:
                        obj = self.view.template(key, table)
                    except KeyError:
                        continue
                    obj.load_sql(ctxid=self.ctxid)
                    obj.state.set('invalid')
                    obj.register()
                    try:
                        await obj.apply()
                    except Exception as e:
                        self.errors.append((time.time(), obj, e))
            for obj in reversed(self.rollback_chain):
                await obj.rollback()
        else:
            apply_script = self._apply_script
            self._apply_script = []
            for op, kwarg in apply_script:
                kwarg['self'] = self
                kwarg['mode'] = mode
                ret = await self.use_db_resync(
                    lambda x: not isinstance(x, KeyError), op, tuple(), kwarg
                )
                if not isinstance(ret, list):
                    ret = [ret]
                for obj in ret:
                    if isinstance(obj, Exception):
                        raise obj
                    elif obj is not None:
                        self._apply_script_snapshots.append(obj)
        return self

    async def use_db_resync(self, criteria, method, argv=None, kwarg=None):
        ret = None
        argv = argv or []
        kwarg = kwarg or {}
        self.log.debug(f'criteria {criteria}')
        self.log.debug(f'method {method}, {argv}, {kwarg}')
        for attempt in range(3):
            ret = []
            for k in method(*argv, **kwarg):
                if isinstance(k, Awaitable):
                    k = await k
                ret.append(k)
            self.log.debug(f'ret {ret}')
            if criteria(ret):
                self.log.debug('criteria matched')
                return ret
            self.log.debug(f'resync the DB attempt {attempt}')
            self.ndb.schema.flush(self['target'])
            self.load_event.clear()
            await self.sources[self['target']].api('dump')
            # await self.evq.put(cmsg_event(self['target'], self.load_event))
            await self.load_event.wait()
            self.load_event.clear()
        return ret

    def update(self, data):
        for key, value in data.items():
            self.load_value(key, value)

    def update_from_sql(self, spec):
        '''
        A bit special case: we can have several fields with
        non unique names.
        '''
        for key, count, value in zip(self.names, self.names_count, spec):
            if count == 1 or value is not None:
                self.load_value(key, value)

    def load_direct(self, key, value):
        super(RTNL_Object, self).__setitem__(key, value)

    def load_value(self, key, value):
        '''
        Load a value and clean up the `self.changed` set if the
        loaded value matches the expectation.
        '''
        if key in self.fields_load_transform:
            value = self.fields_load_transform[key](value)
        if self.load_debug:
            self.log.debug('load %s: %s' % (key, value))
        if key not in self.changed:
            dict.__setitem__(self, key, value)
        elif self.get(key) == value:
            self.changed.remove(key)
        elif key in self.fields_cmp and self.fields_cmp[key](self, value):
            self.changed.remove(key)
        elif self.load_debug:
            self.log.debug(
                'discard %s: %s (expected %s)' % (key, value, self.get(key))
            )

    def load_sql(self, table=None, ctxid=None, set_state=True):
        '''
        Load the data from the database.
        '''
        if not self.key:
            return

        if table is None:
            if ctxid is None:
                table = self.etable
            else:
                table = '%s_%s' % (self.table, ctxid)
        keys = []
        values = []

        for name, value in self.key.items():
            keys.append(f'f_{name} = ?')
            if isinstance(value, (list, tuple, dict)):
                value = json.dumps(value)
            values.append(value)

        spec = self.ndb.schema.fetchone(
            'SELECT * FROM %s WHERE %s' % (table, ' AND '.join(keys)), values
        )
        self.log.debug('load_sql load: %s' % str(spec))
        self.log.debug('load_sql names: %s' % str(self.names))
        if set_state:
            with self.lock:
                if spec is None:
                    if self.state != 'invalid':
                        # No such object (anymore)
                        self.state.set('invalid')
                        self.changed = set()
                elif self.state not in ('remove', 'setns'):
                    self.update_from_sql(spec)
                    self.state.set('system')
        return spec

    async def load_rtnlmsg(self, sources, target, event):
        '''
        Check if the RTNL event matches the object and load the
        data from the database if it does.
        '''
        # TODO: partial match (object rename / restore)
        # ...
        if ObjectFlags.SNAPSHOT in self.flags:
            return

        # full match
        for norm, name in zip(self.knorm, self.kspec):
            value = self.get(norm)
            if value is None:
                continue
            if name == 'target':
                if value != target:
                    return
            elif name == 'tflags':
                continue
            elif value not in (event.get_attr(name), event.get(norm)):
                return

        self.log.debug('load_rtnl: %s' % str(event.get('header')))
        if event['header'].get('type', 0) % 2:
            self.state.set('invalid')
            self.changed = set()
        else:
            self.load_sql()
        self.load_event.set()


class RTNL_Object(SyncBase):

    # FIXME 3.9: Union
    def apply(
        self,
        rollback: bool = False,
        req_filter: Union[None, Callable[[Req], Req]] = None,
        mode: str = 'apply',
    ) -> SyncBase:
        self._main_async_call(self.asyncore.apply, rollback, req_filter, mode)
        return self

    @property
    def state(self):
        return self.asyncore.state

    @property
    def chain(self):
        return self._get_sync_class(
            self.asyncore.chain, key=self.asyncore.chain.table
        )

    @property
    def table(self):
        return self.asyncore.table

    @property
    def etable(self):
        return self.asyncore.etable

    @property
    def key(self):
        return self.asyncore.key

    def complete_key(self, key):
        return self._main_sync_call(self.asyncore.complete_key, key)

    def exists(self, key):
        return self._main_sync_call(self.asyncore.exists, key)

    def load_sql(self, table=None, ctxid=None, set_state=True):
        return self._main_sync_call(
            self.asyncore.load_sql, table, ctxid, set_state
        )

    def load_value(self, key, value):
        return self._main_sync_call(self.asyncore.load_value, key, value)

    def snapshot(self, ctxid=None):
        return self._main_async_call(self.asyncore.snapshot, ctxid)

    def create(self, **spec):
        item = self._main_sync_call(self.asyncore.create, **spec)
        return type(self)(self.event_loop, item)

    def commit(self) -> SyncBase:
        self._main_async_call(self.asyncore.commit)
        return self

    def rollback(self, snapshot=None):
        self._main_async_call(self.asyncore.rollback, snapshot)
        return self

    def show(self, fmt=None):
        return self.asyncore.show(fmt)

    def keys(self):
        return self.asyncore.keys()

    def items(self):
        return self.asyncore.items()

    def set(self, *argv, **kwarg):
        self._main_sync_call(self.asyncore.set, *argv, **kwarg)
        return self

    def get(self, key, *argv):
        return self.asyncore.get(key, *argv)

    def remove(self):
        self.asyncore.remove()
        return self

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.commit()

    def __repr__(self):
        return repr(self.asyncore)

    def __getitem__(self, key):
        return self.asyncore[key]

    def __setitem__(self, key, value):
        return self.set(key, value)
