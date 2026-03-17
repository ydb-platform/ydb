'''
Backends
--------

NDB stores all the records in an SQL database. By default it uses
the SQLite3 module, which is a part of the Python stdlib, so no
extra packages are required::

    # SQLite3 -- simple in-memory DB
    ndb = NDB()

    # SQLite3 -- file DB
    ndb = NDB(db_provider='sqlite3', db_spec='test.db')

It is also possible to use a PostgreSQL database via psycopg2
module::

    # PostgreSQL -- local DB
    ndb = NDB(db_provider='psycopg2',
              db_spec={'dbname': 'test'})

    # PostgreSQL -- remote DB
    ndb = NDB(db_provider='psycopg2',
              db_spec={'dbname': 'test',
                       'host': 'db1.example.com'})

SQL schema
----------

A file based SQLite3 DB or PostgreSQL may be useful for inspection
of the collected data. Here is an example schema::

    rtnl=# \\dt
                List of relations
     Schema |      Name       | Type  | Owner
    --------+-----------------+-------+-------
     public | addresses       | table | root
     public | ifinfo_bond     | table | root
     public | ifinfo_bridge   | table | root
     public | ifinfo_gre      | table | root
     public | ifinfo_vlan     | table | root
     public | ifinfo_vrf      | table | root
     public | ifinfo_vti      | table | root
     public | ifinfo_vti6     | table | root
     public | ifinfo_vxlan    | table | root
     public | interfaces      | table | root
     public | neighbours      | table | root
     public | nh              | table | root
     public | routes          | table | root
     public | sources         | table | root
     public | sources_options | table | root
    (15 rows)

    rtnl=# select f_index, f_ifla_ifname from interfaces;
     f_index | f_ifla_ifname
    ---------+---------------
           1 | lo
           2 | eth0
          28 | ip_vti0
          31 | ip6tnl0
          32 | ip6_vti0
       36445 | br0
       11434 | dummy0
           3 | eth1
    (8 rows)

    rtnl=# select f_index, f_ifla_br_stp_state from ifinfo_bridge;
     f_index | f_ifla_br_stp_state
    ---------+---------------------
       36445 |                   0
    (1 row)

There are also some useful views, that join `ifinfo` tables with
`interfaces`::

    rtnl=# \\dv
           List of relations
     Schema |  Name  | Type | Owner
    --------+--------+------+-------
     public | bond   | view | root
     public | bridge | view | root
     public | gre    | view | root
     public | vlan   | view | root
     public | vrf    | view | root
     public | vti    | view | root
     public | vti6   | view | root
     public | vxlan  | view | root
    (8 rows)

'''
import io
import sys
import time
import random
import sqlite3
import threading
import traceback
from functools import partial
from collections import OrderedDict
from pyroute2 import config
from pyroute2.common import uuid32
from pyroute2.common import basestring
# plugins
from pyroute2.ndb.objects import address
from pyroute2.ndb.objects import interface
from pyroute2.ndb.objects import neighbour
from pyroute2.ndb.objects import netns
from pyroute2.ndb.objects import route
from pyroute2.ndb.objects import rule

#
from pyroute2.ndb.messages import cmsg

try:
    import queue
except ImportError:
    import Queue as queue

#
# the order is important
#
plugins = [interface,
           address,
           neighbour,
           route,
           netns,
           rule]

MAX_ATTEMPTS = 5


def publish(method):
    #
    # this wrapper will be published in the DBM thread
    #
    def _do_local(self, target, request):
        try:
            for item in method(self, *request.argv, **request.kwarg):
                request.response.put(item)
            request.response.put(StopIteration())
        except Exception as e:
            request.response.put(e)

    #
    # this class will be used to map the requests
    #
    class cmsg_req(cmsg):
        def __init__(self, response, *argv, **kwarg):
            self['header'] = {'target': None}
            self.response = response
            self.argv = argv
            self.kwarg = kwarg

    #
    # this method will replace the source one
    #
    def _do_dispatch(self, *argv, **kwarg):
        if self.thread == id(threading.current_thread()):
            # same thread, run method locally
            for item in method(self, *argv, **kwarg):
                yield item
        else:
            # another thread, run via message bus
            self._allow_read.wait()
            response = queue.Queue()
            request = cmsg_req(response, *argv, **kwarg)
            self.ndb._event_queue.put((request, ))
            while True:
                item = response.get()
                if isinstance(item, StopIteration):
                    return
                elif isinstance(item, Exception):
                    raise item
                else:
                    yield item

    #
    # announce the function so it will be published
    #
    _do_dispatch.publish = (cmsg_req, _do_local)

    return _do_dispatch


def publish_exec(method):
    #
    # this wrapper will be published in the DBM thread
    #
    def _do_local(self, target, request):
        try:
            (request
             .response
             .put(method(self, *request.argv, **request.kwarg)))
        except Exception as e:
            (request
             .response
             .put(e))

    #
    # this class will be used to map the requests
    #
    class cmsg_req(cmsg):
        def __init__(self, response, *argv, **kwarg):
            self['header'] = {'target': None}
            self.response = response
            self.argv = argv
            self.kwarg = kwarg

    #
    # this method will replace the source one
    #
    def _do_dispatch(self, *argv, **kwarg):
        if self.thread == id(threading.current_thread()):
            # same thread, run method locally
            return method(self, *argv, **kwarg)
        else:
            # another thread, run via message bus
            response = queue.Queue(maxsize=1)
            request = cmsg_req(response, *argv, **kwarg)
            self.ndb._event_queue.put((request, ))
            ret = response.get()
            if isinstance(ret, Exception):
                raise ret
            else:
                return ret

    #
    # announce the function so it will be published
    #
    _do_dispatch.publish = (cmsg_req, _do_local)

    return _do_dispatch


class DBSchema(object):

    connection = None
    thread = None
    event_map = None
    key_defaults = None
    snapshots = None  # <table_name>: <obj_weakref>

    spec = OrderedDict()
    classes = {}
    #
    # OBS: field names MUST go in the same order as in the spec,
    # that's for the load_netlink() to work correctly -- it uses
    # one loop to fetch both index and row values
    #
    indices = {}
    foreign_keys = {}

    def __init__(self, ndb, connection, mode, rtnl_log, tid):
        self.ndb = ndb
        # collect all the dispatched methods and publish them
        for name in dir(self):
            obj = getattr(self, name, None)
            if hasattr(obj, 'publish'):
                event, fbody = obj.publish
                self.ndb._event_map[event] = [partial(fbody, self)]

        self.mode = mode
        self.stats = {}
        self.thread = tid
        self.connection = connection
        self.rtnl_log = rtnl_log
        self.log = ndb.log.channel('schema')
        self.snapshots = {}
        self.key_defaults = {}
        self.event_map = {}
        self._cursor = None
        self._counter = 0
        self._allow_read = threading.Event()
        self._allow_read.set()
        self._allow_write = threading.Event()
        self._allow_write.set()
        self.share_cursor()
        if self.mode == 'sqlite3':
            # SQLite3
            self.connection.execute('PRAGMA foreign_keys = ON')
            self.plch = '?'
        elif self.mode == 'psycopg2':
            # PostgreSQL
            self.plch = '%s'
        else:
            raise NotImplementedError('database provider not supported')
        self.gctime = self.ctime = time.time()
        #
        # compile request lines
        #
        self.compiled = {}
        for table in self.spec.keys():
            self.compiled[table] = (self
                                    .compile_spec(table,
                                                  self.spec[table],
                                                  self.indices[table]))
            self.create_table(table)

        #
        # service tables
        #
        self.execute('''
                     DROP TABLE IF EXISTS sources_options
                     ''')
        self.execute('''
                     DROP TABLE IF EXISTS sources
                     ''')
        self.execute('''
                     CREATE TABLE IF NOT EXISTS sources
                     (f_target TEXT PRIMARY KEY,
                      f_kind TEXT NOT NULL)
                     ''')
        self.execute('''
                     CREATE TABLE IF NOT EXISTS sources_options
                     (f_target TEXT NOT NULL,
                      f_name TEXT NOT NULL,
                      f_type TEXT NOT NULL,
                      f_value TEXT NOT NULL,
                      FOREIGN KEY (f_target)
                          REFERENCES sources(f_target)
                          ON UPDATE CASCADE
                          ON DELETE CASCADE)
                     ''')

    def merge_spec(self, table1, table2, table, schema_idx):
        spec1 = self.compiled[table1]
        spec2 = self.compiled[table2]
        names = spec1['names'] + spec2['names'][:-1]
        all_names = spec1['all_names'] + spec2['all_names'][2:-1]
        norm_names = spec1['norm_names'] + spec2['norm_names'][2:-1]
        idx = ('target', 'tflags') + schema_idx
        f_names = ['f_%s' % x for x in all_names]
        f_set = ['f_%s = %s' % (x, self.plch) for x in all_names]
        f_idx = ['f_%s' % x for x in idx]
        f_idx_match = ['%s.%s = %s' % (table2, x, self.plch) for x in f_idx]
        plchs = [self.plch] * len(f_names)
        return {'names': names,
                'all_names': all_names,
                'norm_names': norm_names,
                'idx': idx,
                'fnames': ','.join(f_names),
                'plchs': ','.join(plchs),
                'fset': ','.join(f_set),
                'knames': ','.join(f_idx),
                'fidx': ' AND '.join(f_idx_match)}

    def compile_spec(self, table, schema_names, schema_idx):
        # e.g.: index, flags, IFLA_IFNAME
        #
        names = []
        #
        # same + two internal fields
        #
        all_names = ['target', 'tflags']
        #
        #
        norm_names = ['target', 'tflags']

        bclass = self.classes.get(table)

        for name in schema_names:
            names.append(name[-1])
            all_names.append(name[-1])

            iclass = bclass
            if len(name) > 1:
                for step in name[:-1]:
                    imap = dict(iclass.nla_map)
                    iclass = getattr(iclass, imap[step])
            norm_names.append(iclass.nla2name(name[-1]))

        #
        # escaped names: f_index, f_flags, f_IFLA_IFNAME
        #
        # the reason: words like "index" are keywords in SQL
        # and we can not use them; neither can we change the
        # C structure
        #
        f_names = ['f_%s' % x for x in all_names]
        #
        # set the fields
        #
        # e.g.: f_flags = ?, f_IFLA_IFNAME = ?
        #
        # there are different placeholders:
        # ? -- SQLite3
        # %s -- PostgreSQL
        # so use self.plch here
        #
        f_set = ['f_%s = %s' % (x, self.plch) for x in all_names]
        #
        # the set of the placeholders to use in the INSERT statements
        #
        plchs = [self.plch] * len(f_names)
        #
        # the index schema; use target and tflags in every index
        #
        idx = ('target', 'tflags') + schema_idx
        #
        # the same, escaped: f_target, f_tflags etc.
        #
        f_idx = ['f_%s' % x for x in idx]
        #
        # normalized idx names
        #
        norm_idx = [iclass.nla2name(x) for x in idx]
        #
        # match the index fields, fully qualified
        #
        # interfaces.f_index = ?, interfaces.f_IFLA_IFNAME = ?
        #
        # the same issue with the placeholders
        #
        f_idx_match = ['%s.%s = %s' % (table, x, self.plch) for x in f_idx]

        return {'names': names,
                'all_names': all_names,
                'norm_names': norm_names,
                'idx': idx,
                'norm_idx': norm_idx,
                'fnames': ','.join(f_names),
                'plchs': ','.join(plchs),
                'fset': ','.join(f_set),
                'knames': ','.join(f_idx),
                'fidx': ' AND '.join(f_idx_match)}

    @publish_exec
    def execute(self, *argv, **kwarg):
        if self._cursor:
            cursor = self._cursor
        else:
            cursor = self.connection.cursor()
            self._counter = config.db_transaction_limit
        try:
            #
            # FIXME: add logging
            #
            for _ in range(MAX_ATTEMPTS):
                try:
                    self._counter += 1
                    cursor.execute(*argv, **kwarg)
                    break
                except (sqlite3.InterfaceError, sqlite3.OperationalError) as e:
                    self.log.debug('%s' % e)
                    #
                    # Retry on:
                    # -- InterfaceError: Error binding parameter ...
                    # -- OperationalError: SQL logic error
                    #
                    pass
            else:
                raise Exception('DB execute error: %s %s' % (argv, kwarg))
        except Exception:
            self.connection.commit()
            if self._cursor:
                self._cursor = self.connection.cursor()
            raise
        finally:
            if self._counter > config.db_transaction_limit:
                self.connection.commit()  # no performance optimisation yet
                self._counter = 0
        return cursor

    def share_cursor(self):
        self._cursor = self.connection.cursor()
        self._counter = 0

    def unshare_cursor(self):
        self._cursor = None
        self._counter = 0
        self.connection.commit()

    def fetchone(self, *argv, **kwarg):
        for row in self.fetch(*argv, **kwarg):
            return row
        return None

    @publish_exec
    def wait_read(self, timeout=None):
        return self._allow_read.wait(timeout)

    @publish_exec
    def wait_write(self, timeout=None):
        return self._allow_write.wait(timeout)

    def allow_read(self, flag=True):
        if not flag:
            # block immediately...
            self._allow_read.clear()
        # ...then forward the request through the message bus
        # in the case of different threads, or simply run stage2
        self._r_allow_read(flag)

    @publish_exec
    def _r_allow_read(self, flag):
        if flag:
            self._allow_read.set()
        else:
            self._allow_read.clear()

    def allow_write(self, flag=True):
        if not flag:
            self._allow_write.clear()
        self._r_allow_write(flag)

    @publish_exec
    def _r_allow_write(self, flag):
        if flag:
            self._allow_write.set()
        else:
            self._allow_write.clear()

    @publish
    def fetch(self, *argv, **kwarg):
        cursor = self.execute(*argv, **kwarg)
        while True:
            row_set = cursor.fetchmany()
            if not row_set:
                return
            for row in row_set:
                yield row

    @publish_exec
    def export(self, f='stdout'):
        close = False
        if f in ('stdout', 'stderr'):
            f = getattr(sys, f)
        elif isinstance(f, basestring):
            f = open(f, 'w')
            close = True
        try:
            for table in self.spec.keys():
                f.write('\ntable %s\n' % table)
                for record in (self
                               .execute('SELECT * FROM %s' % table)):
                    f.write(' '.join([str(x) for x in record]))
                    f.write('\n')
                if self.rtnl_log:
                    f.write('\ntable %s_log\n' % table)
                    for record in (self
                                   .execute('SELECT * FROM %s_log' % table)):
                        f.write(' '.join([str(x) for x in record]))
                        f.write('\n')
        finally:
            if close:
                f.close()

    def __repr__(self):
        buf = io.StringIO()
        self.export(buf)
        return buf.getvalue()

    @publish_exec
    def close(self):
        self.purge_snapshots()
        self.connection.commit()
        self.connection.close()

    @publish_exec
    def commit(self):
        self.connection.commit()

    def create_table(self, table):
        req = ['f_target TEXT NOT NULL',
               'f_tflags BIGINT NOT NULL DEFAULT 0']
        fields = []
        self.key_defaults[table] = {}
        for field in self.spec[table].items():
            #
            # Why f_?
            # 'Cause there are attributes like 'index' and such
            # names may not be used in SQL statements
            #
            field = (field[0][-1], field[1])
            fields.append('f_%s %s' % field)
            req.append('f_%s %s' % field)
            if field[1].strip().startswith('TEXT'):
                self.key_defaults[table][field[0]] = ''
            else:
                self.key_defaults[table][field[0]] = 0
        if table in self.foreign_keys:
            for key in self.foreign_keys[table]:
                spec = ('(%s)' % ','.join(key['fields']),
                        '%s(%s)' % (key['parent'],
                                    ','.join(key['parent_fields'])))
                req.append('FOREIGN KEY %s REFERENCES %s '
                           'ON UPDATE CASCADE '
                           'ON DELETE CASCADE ' % spec)
                #
                # make a unique index for compound keys on
                # the parent table
                #
                # https://sqlite.org/foreignkeys.html
                #
                if len(key['fields']) > 1:
                    idxname = 'uidx_%s_%s' % (key['parent'],
                                              '_'.join(key['parent_fields']))
                    self.execute('CREATE UNIQUE INDEX '
                                 'IF NOT EXISTS %s ON %s' %
                                 (idxname, spec[1]))

        req = ','.join(req)
        req = ('CREATE TABLE IF NOT EXISTS '
               '%s (%s)' % (table, req))
        # self.execute('DROP TABLE IF EXISTS %s %s'
        #              % (table, 'CASCADE' if self.mode == 'psycopg2' else ''))
        self.execute(req)

        index = ','.join(['f_target', 'f_tflags'] + ['f_%s' % x for x
                                                     in self.indices[table]])
        req = ('CREATE UNIQUE INDEX IF NOT EXISTS '
               '%s_idx ON %s (%s)' % (table, table, index))
        self.execute(req)

        #
        # create table for the transaction buffer: there go the system
        # updates while the transaction is not committed.
        #
        # w/o keys (yet)
        #
        # req = ['f_target TEXT NOT NULL',
        #        'f_tflags INTEGER NOT NULL DEFAULT 0']
        # req = ','.join(req)
        # self.execute('CREATE TABLE IF NOT EXISTS '
        #              '%s_buffer (%s)' % (table, req))
        #
        # create the log table, if required
        #
        if self.rtnl_log:
            req = ['f_tstamp BIGINT NOT NULL',
                   'f_target TEXT NOT NULL',
                   'f_event INTEGER NOT NULL'] + fields
            req = ','.join(req)
            self.execute('CREATE TABLE IF NOT EXISTS '
                         '%s_log (%s)' % (table, req))

    def mark(self, target, mark):
        for table in self.spec:
            self.execute('''
                         UPDATE %s SET f_tflags = %s
                         WHERE f_target = %s
                         ''' % (table, self.plch, self.plch),
                         (mark, target))

    @publish_exec
    def flush(self, target):
        for table in self.spec:
            self.execute('''
                         DELETE FROM %s WHERE f_target = %s
                         ''' % (table, self.plch),
                         (target, ))

    @publish_exec
    def save_deps(self, ctxid, weak_ref, iclass):
        uuid = uuid32()
        obj = weak_ref()
        obj_k = obj.key
        idx = self.indices[obj.table]
        conditions = []
        values = []
        for key in idx:
            conditions.append('f_%s = %s' % (key, self.plch))
            if key in obj_k:
                values.append(obj_k[key])
            else:
                values.append(obj.get(iclass.nla2name(key)))
        #
        # save the old f_tflags value
        #
        tflags = (self
                  .execute('''
                           SELECT f_tflags FROM %s
                           WHERE %s
                           '''
                           % (obj.table,
                              ' AND '.join(conditions)),
                           values)
                  .fetchone()[0])
        #
        # mark tflags for obj
        #
        obj.mark_tflags(uuid)

        #
        # f_tflags is used in foreign keys ON UPDATE CASCADE, so all
        # related records will be marked
        #
        for table in self.spec:
            self.log.debug('create snapshot %s_%s' % (table, ctxid))
            #
            # create the snapshot table
            #
            self.execute('''
                         CREATE TABLE IF NOT EXISTS %s_%s
                         AS SELECT * FROM %s
                         WHERE
                             f_tflags IS NULL
                         '''
                         % (table, ctxid, table))
            #
            # copy the data -- is it possible to do it in one step?
            #
            self.execute('''
                         INSERT INTO %s_%s
                         SELECT * FROM %s
                         WHERE
                             f_tflags = %s
                         '''
                         % (table, ctxid, table, self.plch),
                         [uuid])
        #
        # unmark all the data
        #
        obj.mark_tflags(tflags)

        for table in self.spec:
            self.execute('''
                         UPDATE %s_%s SET f_tflags = %s
                         ''' % (table, ctxid, self.plch),
                         [tflags])
            self.snapshots['%s_%s' % (table, ctxid)] = weak_ref

    def purge_snapshots(self):
        for table in tuple(self.snapshots):
            for _ in range(MAX_ATTEMPTS):
                try:
                    if table.startswith('ifinfo_'):
                        try:
                            self.execute('DROP VIEW %s' % table[7:])
                            self.connection.commit()
                        except Exception:
                            # GC collision?
                            pass
                    if self.mode == 'sqlite3':
                        self.execute('DROP TABLE %s' % table)
                    elif self.mode == 'psycopg2':
                        self.execute('DROP TABLE %s CASCADE' % table)
                    self.connection.commit()
                    del self.snapshots[table]
                    break
                except sqlite3.OperationalError:
                    #
                    # Retry on:
                    # -- OperationalError: database table is locked
                    #
                    time.sleep(random.random())
            else:
                raise Exception('DB snapshot error')

    @publish
    def get(self, table, spec):
        #
        # Retrieve info from the DB
        #
        # ndb.interfaces.get({'ifname': 'eth0'})
        #
        conditions = []
        values = []
        cls = self.classes[table]
        cspec = self.compiled[table]
        for key, value in spec.items():
            if key not in cspec['all_names']:
                key = cls.name2nla(key)
            if key not in cspec['all_names']:
                raise KeyError('field name not found')
            conditions.append('f_%s = %s' % (key, self.plch))
            values.append(value)
        req = 'SELECT * FROM %s WHERE %s' % (table, ' AND '.join(conditions))
        for record in self.fetch(req, values):
            yield dict(zip(self.compiled[table]['all_names'], record))

    def log_netlink(self, table, target, event, ctable=None):
        #
        # RTNL Logs
        #
        fkeys = self.compiled[table]['names']
        fields = ','.join(['f_tstamp', 'f_target', 'f_event'] +
                          ['f_%s' % x for x in fkeys])
        pch = ','.join([self.plch] * (len(fkeys) + 3))
        values = [int(time.time() * 1000),
                  target,
                  event.get('header', {}).get('type', 0)]
        for field in fkeys:
            value = event.get_attr(field) or event.get(field)
            if value is None and field in self.indices[ctable or table]:
                value = self.key_defaults[table][field]
            values.append(value)
        self.execute('INSERT INTO %s_log (%s) VALUES (%s)'
                     % (table, fields, pch), values)

    def load_netlink(self, table, target, event, ctable=None, propagate=False):
        #
        if self.rtnl_log:
            self.log_netlink(table, target, event, ctable)
        #
        # Update metrics
        #
        if 'stats' in event['header']:
            self.stats[target] = event['header']['stats']
        #
        # Periodic jobs
        #
        if time.time() - self.gctime > config.gc_timeout:
            self.gctime = time.time()

            # clean dead snapshots after GC timeout
            for name, wref in tuple(self.snapshots.items()):
                if wref() is None:
                    del self.snapshots[name]
                    if name.startswith('ifinfo_'):
                        try:
                            self.execute('DROP VIEW %s' % name[7:])
                        except Exception as e:
                            self.log.debug(
                                'failed to remove view %s: %s'
                                % (name[7:], e))
                    try:
                        self.execute('DROP TABLE %s' % name)
                    except Exception as e:
                        self.log.debug(
                            'failed to remove table %s: %s'
                            % (name, e))

            # clean marked routes
            self.execute('DELETE FROM routes WHERE '
                         '(f_gc_mark + 5) < %s' % self.plch,
                         (int(time.time()), ))
        #
        # The event type
        #
        if event['header'].get('type', 0) % 2:
            #
            # Delete an object
            #
            conditions = ['f_target = %s' % self.plch]
            values = [target]
            for key in self.indices[table]:
                conditions.append('f_%s = %s' % (key, self.plch))
                value = event.get(key) or event.get_attr(key)
                if value is None:
                    value = self.key_defaults[table][key]
                values.append(value)
            self.execute('DELETE FROM %s WHERE'
                         ' %s' % (table, ' AND '.join(conditions)), values)
        else:
            #
            # Create or set an object
            #
            # field values
            values = [target, 0]
            # index values
            ivalues = [target, 0]
            compiled = self.compiled[table]
            # a map of sub-NLAs
            nodes = {}

            # fetch values (exc. the first two columns)
            for fname, ftype in self.spec[table].items():
                node = event

                # if the field is located in a sub-NLA
                if len(fname) > 1:
                    # see if we tried to get it already
                    if fname[:-1] not in nodes:
                        # descend
                        for steg in fname[:-1]:
                            node = node.get_attr(steg)
                            if node is None:
                                break
                        nodes[fname[:-1]] = node
                    # lookup the sub-NLA in the map
                    node = nodes[fname[:-1]]
                    # the event has no such sub-NLA
                    if node is None:
                        values.append(None)
                        continue

                # NLA have priority
                value = node.get_attr(fname[-1])
                if value is None:
                    value = node.get(fname[-1])
                if value is None and \
                        fname[-1] in self.compiled[table]['idx']:
                    value = self.key_defaults[table][fname[-1]]
                if fname[-1] in compiled['idx']:
                    ivalues.append(value)
                values.append(value)

            try:
                if self.mode == 'psycopg2':
                    #
                    # run UPSERT -- the DB provider must support it
                    #
                    (self
                     .execute('INSERT INTO %s (%s) VALUES (%s) '
                              'ON CONFLICT (%s) '
                              'DO UPDATE SET %s WHERE %s'
                              % (table,
                                 compiled['fnames'],
                                 compiled['plchs'],
                                 compiled['knames'],
                                 compiled['fset'],
                                 compiled['fidx']),
                              (values + values + ivalues)))
                    #
                elif self.mode == 'sqlite3':
                    #
                    # SQLite3 >= 3.24 actually has UPSERT, but ...
                    #
                    # We can not use here INSERT OR REPLACE as well, since
                    # it drops (almost always) records with foreign key
                    # dependencies. Maybe a bug in SQLite3, who knows.
                    #
                    count = (self
                             .execute('''
                                      SELECT count(*) FROM %s WHERE %s
                                      ''' % (table,
                                             compiled['fidx']),
                                      ivalues)
                             .fetchone())[0]
                    if count == 0:
                        self.execute('''
                                     INSERT INTO %s (%s) VALUES (%s)
                                     ''' % (table,
                                            compiled['fnames'],
                                            compiled['plchs']),
                                     values)
                    else:
                        self.execute('''
                                     UPDATE %s SET %s WHERE %s
                                     ''' % (table,
                                            compiled['fset'],
                                            compiled['fidx']),
                                     (values + ivalues))
                else:
                    raise NotImplementedError()
                #
            except Exception as e:
                #
                if propagate:
                    raise e
                #
                # A good question, what should we do here
                self.log.debug('load_netlink: %s %s %s'
                               % (table, target, event))
                self.log.error('load_netlink: %s'
                               % traceback.format_exc())


def init(ndb, connection, mode, rtnl_log, tid):
    #
    # first prepare the schema and init the DB
    #
    for plugin in plugins:
        #
        # 1. spec
        #
        for name, spec in plugin.init['specs']:
            DBSchema.spec[name] = spec.as_dict()
            DBSchema.indices[name] = spec.index
            DBSchema.foreign_keys[name] = spec.foreign_keys
        #
        # 2. classes
        #
        for name, cls in plugin.init['classes']:
            DBSchema.classes[name] = cls

    ret = DBSchema(ndb, connection, mode, rtnl_log, tid)

    #
    # init the event mapping
    #
    for plugin in plugins:
        #
        emap = plugin.init['event_map']
        #
        for etype, ehndl in emap.items():
            handlers = []
            for h in ehndl:
                if isinstance(h, basestring):
                    handlers.append(partial(ret.load_netlink, h))
                else:
                    handlers.append(partial(h, ret))
            ret.event_map[etype] = handlers

    return ret
