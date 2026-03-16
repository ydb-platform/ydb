'''

.. testsetup::

    from pyroute2 import NDB
    from pyroute2 import config
    config.mock_netlink = True
    ndb = NDB()

.. testsetup:: tables

    from pyroute2 import NDB
    from pyroute2 import config
    config.mock_netlink = True
    ndb = NDB()
    ndb.routes.create(
        dst='1.1.1.1/32', gateway='127.0.0.10', oif=1, table=101
    ).commit()
    ndb.routes.create(
        dst='1.1.1.2/32', gateway='127.0.0.10', oif=1, table=5001
    ).commit()
    ndb.routes.create(
        dst='1.1.1.3/32', gateway='127.0.0.10', oif=1, table=5002
    ).commit()

.. testsetup:: metrics

    from pyroute2 import NDB
    from pyroute2 import config
    config.mock_netlink = True
    ndb = NDB()
    ndb.routes.create(
        dst='10.0.0.0/24', gateway='127.0.0.10'
    ).commit()

Simple routes
=============

Ordinary routes management is really simple:

.. testcode::

    # create a route
    ndb.routes.create(
        dst='10.0.0.0/24',
        gateway='192.168.122.1'
    ).commit()

    # retrieve a route and change it
    with ndb.routes['10.0.0.0/24'] as route:
        route.set(gateway='192.168.122.10')

    # remove a route
    with ndb.routes['10.0.0.0/24'] as route:
        route.remove()


Multiple routing tables
=======================

But Linux systems have more than one routing table:

.. doctest:: tables

    >>> set((x.table for x in ndb.routes.summary()))
    {101, 5001, 5002, 254, 255}

The main routing table is 254. All the routes people mostly work with are
in that table. To address routes in other routing tables, you can use dict
specs:

.. testcode::

    ndb.routes.create(
        dst='10.0.0.0/24',
        gateway='192.168.122.1',
        table=101
    ).commit()

    with ndb.routes[{'table': 101, 'dst': '10.0.0.0/24'}] as route:
        route.set('gateway', '192.168.122.10')
        route.set('priority', 500)

    with ndb.routes[{'table': 101, 'dst': '10.0.0.0/24'}] as route:
        route.remove()

Route metrics
=============

`route['metrics']` attribute provides a dictionary-like object that
reflects route metrics like hop limit, mtu etc:

.. testcode:: metrics

    # set up all metrics from a dictionary
    with ndb.routes['10.0.0.0/24'] as route:
        route.set('metrics', {'mtu': 1500, 'hoplimit': 20})

    # fix individual metrics
    with ndb.routes['10.0.0.0/24']['metrics'] as metrics:
        metrics.set('mtu', 1500)
        metrics.set('hoplimit', 20)

MPLS routes
===========

See here: :ref:`mpls`

'''

import json
import struct
import time
import uuid
from collections import OrderedDict
from functools import partial
from socket import AF_INET, inet_pton

from pyroute2.common import AF_MPLS, basestring
from pyroute2.netlink.rtnl.rtmsg import LWTUNNEL_ENCAP_MPLS, nh, rtmsg
from pyroute2.requests.common import MPLSTarget
from pyroute2.requests.route import RouteFieldFilter

from ..objects import AsyncObject, RTNL_Object
from ..report import Record
from ..sync_api import Flags, SyncBase

_dump_rt = ['main.f_%s' % x[0] for x in rtmsg.sql_schema()][:-2]
_dump_nh = ['nh.f_%s' % x[0] for x in nh.sql_schema()][:-2]

F_RTA_MULTIPATH = 1
F_RTA_ENCAP = 2
F_RTA_METRICS = 4


def get_route_id(schema, target, event):
    keys = ['f_target = ?']
    values = [target]
    for key in schema.indices['routes']:
        keys.append(f'f_{key} = ?')
        values.append(event.get(key) or event.get_attr(key))
    #
    spec = 'WHERE %s' % ' AND '.join(keys)
    s_req = 'SELECT f_route_id FROM routes %s' % spec
    #
    # get existing route_id
    for route_id in schema.execute(s_req, values).fetchall():
        #
        # if exists
        return route_id[0][0]
    #
    # or create a new route_id
    return str(uuid.uuid4())


async def load_rtmsg(schema, sources, target, event):
    route_id = None
    post = []

    # fix RTA_TABLE
    rta_table = event.get_attr('RTA_TABLE', -1)
    if rta_table == -1:
        event['attrs'].append(['RTA_TABLE', 254])

    #
    # manage gc marks on related routes
    #
    # only for automatic routes:
    #   - table 254 (main)
    #   - proto 2 (kernel)
    #   - scope 253 (link)
    elif (
        (event.get_attr('RTA_TABLE') == 254)
        and (event['proto'] == 2)
        and (event['scope'] == 253)
        and (event['family'] == AF_INET)
    ):
        evt = event['header']['type']
        #
        # set f_gc_mark = timestamp for "del" events
        # and clean it for "new" events
        #
        try:
            rtmsg_gc_mark(
                schema, target, event, int(time.time()) if (evt % 2) else None
            )
        except Exception as e:
            schema.log.error('gc_mark event: %s' % (event,))
            schema.log.error('gc_mark: %s' % (e,))

    #
    # only for RTM_NEWROUTE events
    #
    if not event['header']['type'] % 2:
        event['deps'] = 0
        #
        # RTA_MULTIPATH
        #
        mp = event.get_attr('RTA_MULTIPATH')
        if mp:
            #
            # create key
            route_id = route_id or get_route_id(schema, target, event)
            #
            # load multipath
            for idx in range(len(mp)):
                mp[idx]['header'] = {}  # for load_netlink()
                mp[idx]['route_id'] = route_id  # set route_id on NH
                mp[idx]['nh_id'] = idx  # add NH number
                post.append(
                    partial(
                        schema.load_netlink,
                        'nh',
                        sources,
                        target,
                        mp[idx],
                        'routes',
                    )
                )
                event['deps'] |= F_RTA_MULTIPATH
        #
        # RTA_ENCAP
        #
        encap = event.get_attr('RTA_ENCAP')
        encap_type = event.get_attr('RTA_ENCAP_TYPE')
        if encap_type == LWTUNNEL_ENCAP_MPLS:
            route_id = route_id or get_route_id(schema, target, event)
            #
            encap['header'] = {}
            encap['route_id'] = route_id
            post.append(
                partial(
                    schema.load_netlink,
                    'enc_mpls',
                    sources,
                    target,
                    encap,
                    'routes',
                )
            )
            event['deps'] |= F_RTA_ENCAP
        #
        # RTA_METRICS
        #
        metrics = event.get_attr('RTA_METRICS')
        if metrics:
            #
            # create key
            route_id = route_id or get_route_id(schema, target, event)
            #
            metrics['header'] = {}
            metrics['route_id'] = route_id
            post.append(
                partial(
                    schema.load_netlink,
                    'metrics',
                    sources,
                    target,
                    metrics,
                    'routes',
                )
            )
            event['deps'] |= F_RTA_METRICS
    #
    if route_id is not None:
        event['route_id'] = route_id
    await schema.load_netlink('routes', sources, target, event)
    #
    for procedure in post:
        await procedure()


def rtmsg_gc_mark(schema, target, event, gc_mark=None):
    #
    if gc_mark is None:
        gc_clause = ' AND f_gc_mark IS NOT NULL'
    else:
        gc_clause = ''
    #
    # select all routes for that OIF where f_gc_mark is not null
    #
    key_fields = ','.join(['f_%s' % x for x in schema.indices['routes']])
    key_query = ' AND '.join([f'f_{x} = ?' for x in schema.indices['routes']])
    routes = schema.execute(
        f'''
           SELECT {key_fields},f_RTA_GATEWAY FROM routes WHERE
           f_target = ? AND f_RTA_OIF = ? AND
           f_RTA_GATEWAY IS NOT NULL {gc_clause} AND
           f_family = 2
        ''',
        (target, event.get_attr('RTA_OIF')),
    ).fetchmany()
    #
    # get the route's RTA_DST and calculate the network
    #
    addr = event.get_attr('RTA_DST')
    net = struct.unpack('>I', inet_pton(AF_INET, addr))[0] & (
        0xFFFFFFFF << (32 - event['dst_len'])
    )
    #
    # now iterate all the routes from the query above and
    # mark those with matching RTA_GATEWAY
    #
    for route in routes:
        # get route GW
        gw = route[-1]
        try:
            gwnet = struct.unpack('>I', inet_pton(AF_INET, gw))[0] & net
            if gwnet == net:
                (
                    schema.execute(
                        f'''
                            UPDATE routes SET f_gc_mark = ?
                            WHERE f_target = ? AND {key_query}
                        ''',
                        (gc_mark, target) + route[:-1],
                    )
                )
        except Exception as e:
            schema.log.error('gc_mark event: %s' % (event,))
            schema.log.error('gc_mark: %s : %s' % (e, route))


rt_schema = (
    rtmsg.sql_schema()
    .push('route_id', 'TEXT UNIQUE')
    .push('gc_mark', 'INTEGER')
    .push('deps', 'INTEGER')
    .unique_index(
        'family',
        'dst_len',
        'tos',
        'scope',
        'type',
        'RTA_DST',
        'RTA_OIF',
        'RTA_PRIORITY',
        'RTA_TABLE',
        'RTA_VIA',
        'RTA_NEWDST',
    )
    .foreign_key(
        'interfaces',
        ('f_target', 'f_tflags', 'f_RTA_OIF'),
        ('f_target', 'f_tflags', 'f_index'),
    )
    .foreign_key(
        'interfaces',
        ('f_target', 'f_tflags', 'f_RTA_IIF'),
        ('f_target', 'f_tflags', 'f_index'),
    )
)

nh_schema = (
    nh.sql_schema()
    .push('route_id', 'TEXT')
    .push('nh_id', 'INTEGER')
    .unique_index('route_id', 'nh_id')
    .foreign_key('routes', ('f_route_id',), ('f_route_id',))
    .foreign_key(
        'interfaces',
        ('f_target', 'f_tflags', 'f_oif'),
        ('f_target', 'f_tflags', 'f_index'),
    )
)

metrics_schema = (
    rtmsg.metrics.sql_schema()
    .push('route_id', 'TEXT')
    .unique_index('route_id')
    .foreign_key('routes', ('f_route_id',), ('f_route_id',))
)

mpls_enc_schema = (
    rtmsg.mpls_encap_info.sql_schema()
    .push('route_id', 'TEXT')
    .unique_index('route_id')
    .foreign_key('routes', ('f_route_id',), ('f_route_id',))
)

init = {
    'specs': [
        ['routes', rt_schema],
        ['nh', nh_schema],
        ['metrics', metrics_schema],
        ['enc_mpls', mpls_enc_schema],
    ],
    'classes': [
        ['routes', rtmsg],
        ['nh', nh],
        ['metrics', rtmsg.metrics],
        ['enc_mpls', rtmsg.mpls_encap_info],
    ],
    'event_map': {rtmsg: [load_rtmsg]},
}


class Via(OrderedDict):
    def __init__(self, prime=None):
        super(OrderedDict, self).__init__()
        if prime is None:
            prime = {}
        elif not isinstance(prime, dict):
            raise TypeError()
        self['family'] = prime.get('family', AF_INET)
        self['addr'] = prime.get('addr', '0.0.0.0')

    def __eq__(self, right):
        return (
            isinstance(right, (dict))
            and self['family'] == right.get('family', AF_INET)
            and self['addr'] == right.get('addr', '0.0.0.0')
        )

    def __repr__(self):
        return repr(dict(self))


class Route(AsyncObject):
    table = 'routes'
    msg_class = rtmsg
    hidden_fields = ['route_id']
    api = 'route'
    field_filter = RouteFieldFilter

    _replace_on_key_change = True

    @classmethod
    def _count(cls, view):
        if view.chain:
            return view.ndb.schema.fetchone(
                f'SELECT count(*) FROM {view.table} WHERE f_RTA_OIF = ?',
                [view.chain['index']],
            )
        else:
            return view.ndb.schema.fetchone(
                f'SELECT count(*) FROM {view.table}'
            )

    @classmethod
    def _dump_where(cls, view):
        if view.chain:
            where = '''
                    WHERE
                        main.f_target = ? AND
                        main.f_RTA_OIF = ?
                    '''
            values = [view.chain['target'], view.chain['index']]
        else:
            where = ''
            values = []
        return (where, values)

    @classmethod
    def summary(cls, view):
        req = '''
              WITH main AS
                  (SELECT
                      nr.f_target, nr.f_tflags, nr.f_RTA_TABLE,
                      nr.f_RTA_DST, nr.f_dst_len,
                      CASE WHEN nh.f_oif > nr.f_RTA_OIF
                          THEN nh.f_oif
                          ELSE nr.f_RTA_OIF
                      END AS f_RTA_OIF,
                      CASE WHEN nh.f_RTA_GATEWAY IS NOT NULL
                          THEN nh.f_RTA_GATEWAY
                          ELSE nr.f_RTA_GATEWAY
                      END AS f_RTA_GATEWAY
                   FROM
                       routes AS nr
                   LEFT JOIN nh
                   ON
                       nr.f_route_id = nh.f_route_id AND
                       nr.f_target = nh.f_target)
              SELECT
                  main.f_target, main.f_tflags, main.f_RTA_TABLE,
                  intf.f_IFLA_IFNAME, main.f_RTA_DST, main.f_dst_len,
                  main.f_RTA_GATEWAY
              FROM
                  main
              INNER JOIN interfaces AS intf
              ON
                  main.f_rta_oif = intf.f_index AND
                  main.f_target = intf.f_target
              '''
        yield (
            'target',
            'tflags',
            'table',
            'ifname',
            'dst',
            'dst_len',
            'gateway',
        )
        where, values = cls._dump_where(view)
        for record in view.ndb.schema.fetch(req + where, values):
            yield record

    @classmethod
    def dump(cls, view):
        req = '''
              SELECT main.f_target,main.f_tflags,%s
              FROM routes AS main
              LEFT JOIN nh AS nh
              ON main.f_route_id = nh.f_route_id
                  AND main.f_target = nh.f_target
              ''' % ','.join(
            ['%s' % x for x in _dump_rt + _dump_nh + ['main.f_route_id']]
        )
        header = (
            ['target', 'tflags']
            + [rtmsg.nla2name(x[7:]) for x in _dump_rt]
            + ['nh_%s' % nh.nla2name(x[5:]) for x in _dump_nh]
            + ['metrics', 'encap']
        )
        yield header
        where, values = cls._dump_where(view)
        for record in view.ndb.schema.fetch(req + where, values):
            route_id = record[-1]
            record = list(record[:-1])
            if route_id is not None:
                #
                # fetch metrics
                metrics = tuple(
                    view.ndb.schema.fetch(
                        'SELECT * FROM metrics WHERE f_route_id = ?',
                        (route_id,),
                    )
                )
                if metrics:
                    ret = {}
                    names = view.ndb.schema.compiled['metrics']['norm_names']
                    for k, v in zip(names, metrics[0]):
                        if v is not None and k not in (
                            'target',
                            'route_id',
                            'tflags',
                        ):
                            ret[k] = v
                    record.append(json.dumps(ret))
                else:
                    record.append(None)
                #
                # fetch encap
                enc_mpls = tuple(
                    view.ndb.schema.fetch(
                        'SELECT * FROM enc_mpls WHERE f_route_id = ?',
                        (route_id,),
                    )
                )
                if enc_mpls:
                    record.append(enc_mpls[0][2])
                else:
                    record.append(None)
            else:
                record.extend((None, None))
            yield record

    @classmethod
    def spec_normalize(cls, spec):
        if isinstance(spec, basestring):
            return {'dst': spec}
        return spec

    @classmethod
    def compare_record(self, left, right):
        if isinstance(right, str):
            return right == f'{left["dst"]}/{left["dst_len"]}'

    def _cmp_target(key, self, right):
        right = [MPLSTarget(x) for x in json.loads(right)]
        return all([x[0] == x[1] for x in zip(self[key], right)])

    def _cmp_via(self, right):
        return self['via'] == Via(json.loads(right))

    def _cmp_encap(self, right):
        return all([x[0] == x[1] for x in zip(self.get('encap', []), right)])

    fields_cmp = {
        'dst': partial(_cmp_target, 'dst'),
        'src': partial(_cmp_target, 'src'),
        'newdst': partial(_cmp_target, 'newdst'),
        'encap': _cmp_encap,
        'via': _cmp_via,
    }

    def mark_tflags(self, mark):
        self.schema.execute(
            '''
               UPDATE interfaces SET
                   f_tflags = ?
               WHERE
                   (f_index = ? OR f_index = ?)
                   AND f_target = ?
            ''',
            (mark, self['iif'], self['oif'], self['target']),
        )

    def __init__(self, *argv, **kwarg):
        kwarg['iclass'] = rtmsg
        self.event_map = {rtmsg: "load_rtnlmsg"}
        dict.__setitem__(self, 'multipath', [])
        dict.__setitem__(self, 'metrics', MetricsStub(self))
        dict.__setitem__(self, 'deps', 0)
        super(Route, self).__init__(*argv, **kwarg)

    def complete_key(self, key):
        ret_key = {}
        if isinstance(key, basestring):
            ret_key['dst'] = key
        elif isinstance(key, (Record, tuple, list)):
            return super(Route, self).complete_key(key)
        elif isinstance(key, dict):
            ret_key.update(key)
        else:
            raise TypeError('unsupported key type')

        if 'target' not in ret_key:
            ret_key['target'] = self.ndb.config.localhost

        ##
        # previously here was a code that injected the default
        # table == 254 into the key:
        #
        # table = ret_key.get('table', ret_key.get('RTA_TABLE', 254))
        # if 'table' not in ret_key:
        #     ret_key['table'] = table
        #
        # the issue with the code is that self.exists() didn't use
        # it, thus it was possible to get self.exists() == True and
        # at the same time loading from the DB resulted in an empty
        # record
        #
        # probably more correct behaviour would be to raise KeyError
        # if a route spec has no table defined, and the route is
        # in another table than 254; but for now routes['ipaddr/mask']
        # returns records even outside of the main table

        if isinstance(ret_key.get('dst_len'), basestring):
            ret_key['dst_len'] = int(ret_key['dst_len'])

        if isinstance(ret_key.get('dst'), basestring):
            if ret_key.get('dst') == 'default':
                ret_key['dst'] = ''
                ret_key['dst_len'] = 0
            elif '/' in ret_key['dst']:
                ret_key['dst'], ret_key['dst_len'] = ret_key['dst'].split('/')

        if ret_key.get('family', 0) == AF_MPLS:
            for field in ('dst', 'src', 'newdst', 'via'):
                value = ret_key.get(field, key.get(field, None))
                if isinstance(value, (list, tuple, dict)):
                    ret_key[field] = json.dumps(value)

        return super(Route, self).complete_key(ret_key)

    @property
    def clean(self):
        clean = True
        for s in (self['metrics'],) + tuple(self['multipath']):
            if hasattr(s, 'changed'):
                clean &= len(s.changed) == 0
        return clean & super(Route, self).clean

    def make_req(self, prime):
        req = dict(prime)
        for key in self.changed:
            req[key] = self[key]
        if self['multipath']:
            req['multipath'] = self['multipath']
        if self['metrics']:
            req['metrics'] = self['metrics']
        if self.get('encap') and self.get('encap_type'):
            req['encap'] = {
                'type': self['encap_type'],
                'labels': self['encap'],
            }
        if self.get('gateway'):
            req['gateway'] = self['gateway']
        return req

    def __setitem__(self, key, value):
        if key == 'route_id':
            raise ValueError('route_id is read only')
        elif key == 'multipath':
            super(Route, self).__setitem__('multipath', [])
            for mp in value:
                mp = dict(mp)
                if self.state == 'invalid':
                    mp['create'] = True
                obj = NextHop(self, self.view, mp)
                obj.state.set(self.state.get())
                self['multipath'].append(obj)
            if key in self.changed:
                self.changed.remove(key)
        elif key == 'metrics':
            value = dict(value)
            if not isinstance(self['metrics'], Metrics):
                value['create'] = True
            obj = Metrics(self, self.view, value)
            obj.state.set(self.state.get())
            super(Route, self).__setitem__('metrics', obj)
            if key in self.changed:
                self.changed.remove(key)
        elif self.get('family', 0) == AF_MPLS and key in (
            'dst',
            'src',
            'newdst',
        ):
            if isinstance(value, (dict, int)):
                value = [value]
            na = []
            target = None
            for label in value:
                target = MPLSTarget(label)
                target['bos'] = 0
                na.append(target)
            target['bos'] = 1
            super(Route, self).__setitem__(key, na)
        else:
            super(Route, self).__setitem__(key, value)

    def apply(self, rollback=False, req_filter=None, mode='apply'):
        if (
            (self.get('table') == 255)
            and (self.get('family') == 10)
            and (self.get('proto') == 2)
        ):
            # skip automatic ipv6 routes with proto kernel
            return self
        else:
            if self.get('family', AF_INET) == AF_MPLS and not self.get('dst'):
                dict.__setitem__(self, 'dst', [MPLSTarget()])
            return super(Route, self).apply(rollback, req_filter, mode)

    def load_sql(self, *argv, **kwarg):
        super(Route, self).load_sql(*argv, **kwarg)
        # transform MPLS
        if self.get('family', 0) == AF_MPLS:
            for field in ('newdst', 'dst', 'src', 'via'):
                value = self.get(field, None)
                if isinstance(value, basestring) and value != '':
                    if field == 'via':
                        na = json.loads(value)
                    else:
                        na = [MPLSTarget(x) for x in json.loads(value)]
                    dict.__setitem__(self, field, na)
        #
        # fetch encap deps
        if self['deps'] & F_RTA_ENCAP:
            for _ in range(5):
                enc = tuple(
                    self.schema.fetch(
                        'SELECT * FROM enc_mpls WHERE f_route_id = ?',
                        (self['route_id'],),
                    )
                )
                if enc:
                    na = [MPLSTarget(x) for x in json.loads(enc[0][2])]
                    self.load_value('encap', na)
                    break
                time.sleep(0.1)
            else:
                self.log.error('no encap loaded for %s' % (self['route_id'],))
        #
        #
        if not self.load_event.is_set():
            return
        #
        # fetch metrics
        if self['deps'] & F_RTA_METRICS:
            for _ in range(5):
                metrics = tuple(
                    self.schema.fetch(
                        'SELECT * FROM metrics WHERE f_route_id = ?',
                        (self['route_id'],),
                    )
                )
                if metrics:
                    self['metrics'] = Metrics(
                        self, self.view, {'route_id': self['route_id']}
                    )
                    break
                time.sleep(0.1)
            else:
                self.log.error(
                    'no metrics loaded for %s' % (self['route_id'],)
                )
        #
        # fetch multipath
        #
        # FIXME: use self['deps']
        if 'nh_id' not in self and self.get('route_id') is not None:
            nhs = iter(
                tuple(
                    self.schema.fetch(
                        'SELECT * FROM nh WHERE f_route_id = ?',
                        (self['route_id'],),
                    )
                )
            )

            flush = False
            idx = 0
            for nexthop in tuple(self['multipath']):
                if not isinstance(nexthop, NextHop):
                    flush = True

                if not flush:
                    try:
                        spec = next(nhs)
                    except StopIteration:
                        flush = True
                    for key, value in zip(nexthop.names, spec):
                        if key in nexthop and value is None:
                            continue
                        else:
                            nexthop.load_value(key, value)
                if flush:
                    self['multipath'].pop(idx)
                    continue
                idx += 1

            for nexthop in nhs:
                key = {'route_id': self['route_id'], 'nh_id': nexthop[-1]}
                (self['multipath'].append(NextHop(self, self.view, key)))


class NextHop(AsyncObject):
    msg_class = nh
    table = 'nh'
    hidden_fields = ('route_id', 'target')

    def mark_tflags(self, mark):
        self.schema.execute(
            '''
               UPDATE interfaces SET
                   f_tflags = ?
               WHERE
                   (f_index = ? OR f_index = ?)
                   AND f_target = ?
            ''',
            (mark, self.route['iif'], self.route['oif'], self.route['target']),
        )

    def __init__(self, route, *argv, **kwarg):
        self.route = route
        kwarg['iclass'] = nh
        kwarg['check'] = False
        super(NextHop, self).__init__(*argv, **kwarg)


class MetricsStub(dict):
    def __init__(self, route):
        self.route = route

    def __setitem__(self, key, value):
        # This assignment forces the Metrics object to replace
        # MetricsStub; it is the MetricsStub object end of life
        self.route['metrics'] = {key: value}

    def __getitem__(self, key):
        raise KeyError('metrics not initialized for this route')


class Metrics(AsyncObject):
    msg_class = rtmsg.metrics
    table = 'metrics'
    hidden_fields = ('route_id', 'target')

    def mark_tflags(self, mark):
        self.schema.execute(
            '''
               UPDATE interfaces SET
                   f_tflags = ?
               WHERE
                   (f_index = ? OR f_index = ?)
                   AND f_target = ?
            ''',
            (mark, self.route['iif'], self.route['oif'], self.route['target']),
        )

    def __init__(self, route, *argv, **kwarg):
        self.route = route
        kwarg['iclass'] = rtmsg.metrics
        kwarg['check'] = False
        super(Metrics, self).__init__(*argv, **kwarg)


class RouteSub(SyncBase):
    def apply(self, rollback=False, req_filter=None, mode='apply'):
        return self.route.apply(rollback, req_filter, mode)

    def commit(self):
        return self.route.commit()

    def set(self, key, value):
        self[key] = value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.commit()

    def __getitem__(self, key):
        return self._main_sync_call(self.asyncore.__getitem__, key)

    def __setitem__(self, key, value):
        return self._main_sync_call(self.asyncore.__setitem__, key, value)


class SyncRoute(RTNL_Object):

    def __init__(self, event_loop, asyncore, class_map=None, flags=Flags.RO):
        super().__init__(event_loop, asyncore, class_map, flags)

    @property
    def metrics(self):
        metrics = self._main_sync_call(self.asyncore.__getitem__, 'metrics')
        ret = RouteSub(self.event_loop, metrics, self.class_map, self.flags)
        ret.route = self
        return ret

    @property
    def multipath(self):
        mp = self._main_sync_call(self.asyncore.__getitem__, 'multipath')
        ret = []
        for nexthop in mp:
            rs = RouteSub(self.event_loop, nexthop, self.class_map, self.flags)
            rs.route = self
            ret.append(rs)
        return ret

    def __getitem__(self, key):
        if key == 'metrics':
            return self.metrics
        if key == 'multipath':
            return self.multipath
        return super().__getitem__(key)

    def get(self, key, *argv):
        if key in ('metrics', 'multipath'):
            return self[key]
        return super().get(key, *argv)
