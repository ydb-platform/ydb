'''
Simple routes
=============

Ordinary routes management is really simple::

    (ndb            # create a route
     .routes
     .create(dst='10.0.0.0/24', gateway='192.168.122.1')
     .commit())

    (ndb            # retrieve a route and change it
     .routes['10.0.0.0/24']
     .set('gateway', '192.168.122.10')
     .set('priority', 500)
     .commit())

    (ndb            # remove a route
     .routes['10.0.0.0/24']
     .remove()
     .commit())


Multiple routing tables
=======================

But Linux systems have more than one routing table::

    >>> set((x.table for x in ndb.routes.summary()))
    {101, 254, 255, 5001, 5002}

The main routing table is 254. All the routes people mostly work with are
in that table. To address routes in other routing tables, you can use dict
specs::

    (ndb
     .routes
     .create(dst='10.0.0.0/24', gateway='192.168.122.1', table=101)
     .commit())

    (ndb
     .routes[{'table': 101, 'dst': '10.0.0.0/24'}]
     .set('gateway', '192.168.122.10')
     .set('priority', 500)
     .commit())

    (ndb
     .routes[{'table': 101, 'dst': '10.0.0.0/24'}]
     .remove()
     .commit())

Route metrics
=============

`route['metrics']` attribute provides a dictionary-like object that
reflects route metrics like hop limit, mtu etc::

    # set up all metrics from a dictionary
    (ndb
     .routes['10.0.0.0/24']
     .set('metrics', {'mtu': 1500, 'hoplimit': 20})
     .commit())

    # fix individual metrics
    (ndb
     .routes['10.0.0.0/24']['metrics']
     .set('mtu', 1500)
     .set('hoplimit', 20)
     .commit())

MPLS routes
===========

See here: :ref:`mpls`

'''
import time
import uuid
import json
import struct
from socket import AF_INET
from socket import AF_INET6
from socket import inet_pton
from functools import partial
from collections import OrderedDict
from pyroute2.ndb.objects import RTNL_Object
from pyroute2.ndb.report import Record
from pyroute2.ndb.auth_manager import check_auth
from pyroute2.common import basestring
from pyroute2.common import AF_MPLS
from pyroute2.netlink.rtnl.rtmsg import rtmsg
from pyroute2.netlink.rtnl.rtmsg import nh
from pyroute2.netlink.rtnl.rtmsg import LWTUNNEL_ENCAP_MPLS

_dump_rt = ['main.f_%s' % x[0] for x in rtmsg.sql_schema()][:-2]
_dump_nh = ['nh.f_%s' % x[0] for x in nh.sql_schema()][:-2]


def get_route_id(schema, target, event):
    keys = ['f_target = %s' % schema.plch]
    values = [target]
    for key in schema.indices['routes']:
        keys.append('f_%s = %s' % (key, schema.plch))
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


def load_rtmsg(schema, target, event):
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
    elif (event.get_attr('RTA_TABLE') == 254) and \
            (event['proto'] == 2) and \
            (event['scope'] == 253) and \
            (event['family'] == AF_INET):
        evt = event['header']['type']
        #
        # set f_gc_mark = timestamp for "del" events
        # and clean it for "new" events
        #
        try:
            rtmsg_gc_mark(schema, target, event,
                          int(time.time()) if (evt % 2) else None)
        except Exception as e:
            schema.log.error('gc_mark event: %s' % (event, ))
            schema.log.error('gc_mark: %s' % (e, ))

    #
    # only for RTM_NEWROUTE events
    #
    if not event['header']['type'] % 2:
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
                mp[idx]['header'] = {}          # for load_netlink()
                mp[idx]['route_id'] = route_id  # set route_id on NH
                mp[idx]['nh_id'] = idx          # add NH number
                post.append(partial(schema.load_netlink, 'nh', target,
                                    mp[idx], 'routes'))
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
            post.append(partial(schema.load_netlink, 'enc_mpls', target,
                                encap, 'routes'))
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
            post.append(partial(schema.load_netlink, 'metrics', target,
                                metrics, 'routes'))
    #
    if route_id is not None:
        event['route_id'] = route_id
    schema.load_netlink('routes', target, event)
    #
    for procedure in post:
        procedure()


def rtmsg_gc_mark(schema, target, event, gc_mark=None):
    #
    if gc_mark is None:
        gc_clause = ' AND f_gc_mark IS NOT NULL'
    else:
        gc_clause = ''
    #
    # select all routes for that OIF where f_gc_mark is not null
    #
    key_fields = ','.join(['f_%s' % x for x
                           in schema.indices['routes']])
    key_query = ' AND '.join(['f_%s = %s' % (x, schema.plch) for x
                              in schema.indices['routes']])
    routes = (schema
              .execute('''
                       SELECT %s,f_RTA_GATEWAY FROM routes WHERE
                       f_target = %s AND f_RTA_OIF = %s AND
                       f_RTA_GATEWAY IS NOT NULL %s AND
                       f_family = 2
                       ''' % (key_fields,
                              schema.plch,
                              schema.plch,
                              gc_clause),
                       (target, event.get_attr('RTA_OIF')))
              .fetchmany())
    #
    # get the route's RTA_DST and calculate the network
    #
    addr = event.get_attr('RTA_DST')
    net = struct.unpack('>I', inet_pton(AF_INET, addr))[0] &\
        (0xffffffff << (32 - event['dst_len']))
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
                (schema
                 .execute('UPDATE routes SET f_gc_mark = %s '
                          'WHERE f_target = %s AND %s'
                          % (schema.plch, schema.plch, key_query),
                          (gc_mark, target) + route[:-1]))
        except Exception as e:
            schema.log.error('gc_mark event: %s' % (event, ))
            schema.log.error('gc_mark: %s : %s' % (e, route))


rt_schema = (rtmsg
             .sql_schema()
             .push('route_id', 'TEXT UNIQUE')
             .push('gc_mark', 'INTEGER')
             .unique_index('family',
                           'dst_len',
                           'tos',
                           'scope',
                           'RTA_DST',
                           'RTA_OIF',
                           'RTA_PRIORITY',
                           'RTA_TABLE',
                           'RTA_VIA',
                           'RTA_NEWDST')
             .foreign_key('interfaces',
                          ('f_target', 'f_tflags', 'f_RTA_OIF'),
                          ('f_target', 'f_tflags', 'f_index'))
             .foreign_key('interfaces',
                          ('f_target', 'f_tflags', 'f_RTA_IIF'),
                          ('f_target', 'f_tflags', 'f_index')))

nh_schema = (nh
             .sql_schema()
             .push('route_id', 'TEXT')
             .push('nh_id', 'INTEGER')
             .unique_index('route_id', 'nh_id')
             .foreign_key('routes', ('f_route_id', ), ('f_route_id', ))
             .foreign_key('interfaces',
                          ('f_target', 'f_tflags', 'f_oif'),
                          ('f_target', 'f_tflags', 'f_index')))

metrics_schema = (rtmsg
                  .metrics
                  .sql_schema()
                  .push('route_id', 'TEXT')
                  .unique_index('route_id')
                  .foreign_key('routes', ('f_route_id', ), ('f_route_id', )))

mpls_enc_schema = (rtmsg
                   .mpls_encap_info
                   .sql_schema()
                   .push('route_id', 'TEXT')
                   .unique_index('route_id')
                   .foreign_key('routes', ('f_route_id', ), ('f_route_id', )))

init = {'specs': [['routes', rt_schema],
                  ['nh', nh_schema],
                  ['metrics', metrics_schema],
                  ['enc_mpls', mpls_enc_schema]],
        'classes': [['routes', rtmsg],
                    ['nh', nh],
                    ['metrics', rtmsg.metrics],
                    ['enc_mpls', rtmsg.mpls_encap_info]],
        'event_map': {rtmsg: [load_rtmsg]}}


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
        return isinstance(right, (dict, Target)) and \
            self['family'] == right.get('family', AF_INET) and \
            self['addr'] == right.get('addr', '0.0.0.0')

    def __repr__(self):
        return repr(dict(self))


class Target(OrderedDict):

    def __init__(self, prime=None):
        super(OrderedDict, self).__init__()
        if prime is None:
            prime = {}
        elif isinstance(prime, int):
            prime = {'label': prime}
        elif isinstance(prime, dict):
            pass
        else:
            raise TypeError()
        self['label'] = prime.get('label', 16)
        self['tc'] = prime.get('tc', 0)
        self['bos'] = prime.get('bos', 1)
        self['ttl'] = prime.get('ttl', 0)

    def __eq__(self, right):
        return isinstance(right, (dict, Target)) and \
            self['label'] == right.get('label', 16) and \
            self['tc'] == right.get('tc', 0) and \
            self['bos'] == right.get('bos', 1) and \
            self['ttl'] == right.get('ttl', 0)

    def __repr__(self):
        return repr(dict(self))


class Route(RTNL_Object):

    table = 'routes'
    msg_class = rtmsg
    hidden_fields = ['route_id']
    api = 'route'

    _replace_on_key_change = True

    @classmethod
    def _dump_where(cls, view):
        if view.chain:
            plch = view.ndb.schema.plch
            where = '''
                    WHERE
                        main.f_target = %s AND
                        main.f_RTA_OIF = %s
                    ''' % (plch, plch)
            values = [view.chain['target'], view.chain['index']]
        else:
            where = ''
            values = []
        return (where, values)

    @classmethod
    def summary(cls, view):
        req = '''
              WITH nr AS
                  (SELECT
                      main.f_target, main.f_tflags, main.f_RTA_TABLE,
                      main.f_RTA_DST, main.f_dst_len,
                      CASE WHEN nh.f_oif > main.f_RTA_OIF
                          THEN nh.f_oif
                          ELSE main.f_RTA_OIF
                      END AS f_RTA_OIF,
                      CASE WHEN nh.f_RTA_GATEWAY IS NOT NULL
                          THEN nh.f_RTA_GATEWAY
                          ELSE main.f_RTA_GATEWAY
                      END AS f_RTA_GATEWAY
                   FROM
                       routes AS main
                   LEFT JOIN nh
                   ON
                       main.f_route_id = nh.f_route_id AND
                       main.f_target = nh.f_target)
              SELECT
                  nr.f_target, nr.f_tflags, nr.f_RTA_TABLE,
                  intf.f_IFLA_IFNAME, nr.f_RTA_DST, nr.f_dst_len,
                  nr.f_RTA_GATEWAY
              FROM
                  nr
              INNER JOIN interfaces AS intf
              ON
                  nr.f_rta_oif = intf.f_index AND
                  nr.f_target = intf.f_target
              '''
        yield ('target', 'tflags', 'table', 'ifname',
               'dst', 'dst_len', 'gateway')
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
              ''' % ','.join(['%s' % x for x in
                              _dump_rt + _dump_nh + ['main.f_route_id']])
        header = (['target', 'tflags'] +
                  [rtmsg.nla2name(x[7:]) for x in _dump_rt] +
                  ['nh_%s' % nh.nla2name(x[5:]) for x in _dump_nh] +
                  ['metrics', 'encap'])
        yield header
        plch = view.ndb.schema.plch
        where, values = cls._dump_where(view)
        for record in view.ndb.schema.fetch(req + where, values):
            route_id = record[-1]
            record = list(record[:-1])
            #
            # fetch metrics
            metrics = tuple(view.ndb.schema.fetch('''
                SELECT * FROM metrics WHERE f_route_id = %s
            ''' % (plch, ), (route_id, )))
            if metrics:
                ret = {}
                names = view.ndb.schema.compiled['metrics']['norm_names']
                for k, v in zip(names, metrics[0]):
                    if v is not None and \
                            k not in ('target', 'route_id', 'tflags'):
                        ret[k] = v
                record.append(json.dumps(ret))
            else:
                record.append(None)
            #
            # fetch encap
            enc_mpls = tuple(view.ndb.schema.fetch('''
                SELECT * FROM enc_mpls WHERE f_route_id = %s
            ''' % (plch, ), (route_id, )))
            if enc_mpls:
                record.append(enc_mpls[0][2])
            else:
                record.append(None)
            yield record

    @staticmethod
    def spec_normalize(spec):
        if isinstance(spec, dict):
            ret = dict(spec)
        elif isinstance(spec, basestring):
            dst_spec = spec.split('/')
            ret = {'dst': dst_spec[0]}
            if len(dst_spec) == 2:
                ret['dst_len'] = int(dst_spec[1])
            elif len(dst_spec) > 2:
                raise TypeError('invalid spec format')
        else:
            raise TypeError('invalid spec type')
        if ret.get('dst') == 'default':
            ret['dst'] = ''
        elif ret.get('dst') in ('::', '::/0'):
            ret['dst'] = ''
            ret['family'] = AF_INET6
        return ret

    def _cmp_target(key, self, right):
        right = [Target(x) for x in json.loads(right)]
        return all([x[0] == x[1] for x in zip(self[key], right)])

    def _cmp_via(self, right):
        return self['via'] == Via(json.loads(right))

    def _cmp_encap(self, right):
        return all([x[0] == x[1] for x in zip(self.get('encap', []), right)])

    fields_cmp = {'dst': partial(_cmp_target, 'dst'),
                  'src': partial(_cmp_target, 'src'),
                  'newdst': partial(_cmp_target, 'newdst'),
                  'encap': _cmp_encap,
                  'via': _cmp_via}

    def mark_tflags(self, mark):
        plch = (self.schema.plch, ) * 4
        self.schema.execute('''
                            UPDATE interfaces SET
                                f_tflags = %s
                            WHERE
                                (f_index = %s OR f_index = %s)
                                AND f_target = %s
                            ''' % plch,
                            (mark,
                             self['iif'],
                             self['oif'],
                             self['target']))

    def __init__(self, *argv, **kwarg):
        kwarg['iclass'] = rtmsg
        self.event_map = {rtmsg: "load_rtnlmsg"}
        dict.__setitem__(self, 'multipath', [])
        dict.__setitem__(self, 'metrics', {})
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
            ret_key['target'] = self.ndb.localhost

        table = ret_key.get('table', ret_key.get('RTA_TABLE', 254))
        if 'table' not in ret_key:
            ret_key['table'] = table

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
        for s in (self['metrics'], ) + tuple(self['multipath']):
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
            req['encap'] = {'type': self['encap_type'],
                            'labels': self['encap']}
        if self.get('gateway'):
            req['gateway'] = self['gateway']
        return req

    @check_auth('obj:modify')
    def __setitem__(self, key, value):
        if key in ('dst', 'src') and \
                isinstance(value, basestring) and \
                '/' in value:
            net, net_len = value.split('/')
            if net in ('0', '0.0.0.0'):
                net = ''
            super(Route, self).__setitem__(key, net)
            super(Route, self).__setitem__('%s_len' % key, int(net_len))
        elif key == 'dst' and value == 'default':
            super(Route, self).__setitem__('dst', '')
            super(Route, self).__setitem__('dst_len', 0)
        elif key == 'route_id':
            raise ValueError('route_id is read only')
        elif key == 'multipath':
            super(Route, self).__setitem__('multipath', [])
            for mp in value:
                mp = dict(mp)
                if self.state == 'invalid':
                    mp['create'] = True
                obj = NextHop(self,
                              self.view,
                              mp,
                              auth_managers=self.auth_managers)
                obj.state.set(self.state.get())
                self['multipath'].append(obj)
            if key in self.changed:
                self.changed.remove(key)
        elif key == 'metrics':
            value = dict(value)
            if self.state == 'invalid':
                value['create'] = True
            obj = Metrics(self,
                          self.view,
                          value,
                          auth_managers=self.auth_managers)
            obj.state.set(self.state.get())
            super(Route, self).__setitem__('metrics', obj)
            if key in self.changed:
                self.changed.remove(key)
        elif key == 'encap':
            if value.get('type') == 'mpls':
                na = []
                target = None
                value = value.get('labels', [])
                if isinstance(value, (dict, int)):
                    value = [value, ]
                for label in value:
                    target = Target(label)
                    target['bos'] = 0
                    na.append(target)
                target['bos'] = 1
                super(Route, self).__setitem__('encap_type',
                                               LWTUNNEL_ENCAP_MPLS)
                super(Route, self).__setitem__('encap', na)
        elif self.get('family', 0) == AF_MPLS and \
                key in ('dst', 'src', 'newdst'):
            if isinstance(value, (dict, int)):
                value = [value, ]
            na = []
            target = None
            for label in value:
                target = Target(label)
                target['bos'] = 0
                na.append(target)
            target['bos'] = 1
            super(Route, self).__setitem__(key, na)
        else:
            super(Route, self).__setitem__(key, value)

    @check_auth('obj:modify')
    def apply(self, rollback=False):
        if (self.get('table') == 255) and \
                (self.get('family') == 10) and \
                (self.get('proto') == 2):
            # skip automatic ipv6 routes with proto kernel
            return self
        else:
            if self.get('family', AF_INET) == AF_MPLS and not self.get('dst'):
                dict.__setitem__(self, 'dst', [Target()])
            return super(Route, self).apply(rollback)

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
                        na = [Target(x) for x in json.loads(value)]
                    dict.__setitem__(self, field, na)

        if self.get('route_id') is not None:
            enc = (self
                   .schema
                   .fetch('SELECT * FROM enc_mpls WHERE f_route_id = %s' %
                          (self.schema.plch, ), (self['route_id'], )))
            enc = tuple(enc)
            if enc:
                na = [Target(x) for x in json.loads(enc[0][2])]
                self.load_value('encap', na)

        if not self.load_event.is_set():
            return

        if 'nh_id' not in self and self.get('route_id') is not None:
            nhs = (self
                   .schema
                   .fetch('SELECT * FROM nh WHERE f_route_id = %s' %
                          (self.schema.plch, ), (self['route_id'], )))
            metrics = (self
                       .schema
                       .fetch('SELECT * FROM metrics WHERE f_route_id = %s' %
                              (self.schema.plch, ), (self['route_id'], )))

            if tuple(metrics):
                self['metrics'] = Metrics(self,
                                          self.view,
                                          {'route_id': self['route_id']},
                                          auth_managers=self.auth_managers)

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
                key = {'route_id': self['route_id'],
                       'nh_id': nexthop[-1]}
                (self['multipath']
                 .append(NextHop(self,
                                 self.view,
                                 key,
                                 auth_managers=self.auth_managers)))


class RouteSub(object):

    def apply(self, *argv, **kwarg):
        return self.route.apply(*argv, **kwarg)

    def commit(self, *argv, **kwarg):
        return self.route.commit(*argv, **kwarg)


class NextHop(RouteSub, RTNL_Object):

    msg_class = nh
    table = 'nh'
    hidden_fields = ('route_id', 'target')

    def mark_tflags(self, mark):
        plch = (self.schema.plch, ) * 4
        self.schema.execute('''
                            UPDATE interfaces SET
                                f_tflags = %s
                            WHERE
                                (f_index = %s OR f_index = %s)
                                AND f_target = %s
                            ''' % plch,
                            (mark,
                             self.route['iif'],
                             self.route['oif'],
                             self.route['target']))

    def __init__(self, route, *argv, **kwarg):
        self.route = route
        kwarg['iclass'] = nh
        kwarg['check'] = False
        super(NextHop, self).__init__(*argv, **kwarg)


class Metrics(RouteSub, RTNL_Object):

    msg_class = rtmsg.metrics
    table = 'metrics'
    hidden_fields = ('route_id', 'target')

    def mark_tflags(self, mark):
        plch = (self.schema.plch, ) * 4
        self.schema.execute('''
                            UPDATE interfaces SET
                                f_tflags = %s
                            WHERE
                                (f_index = %s OR f_index = %s)
                                AND f_target = %s
                            ''' % plch,
                            (mark,
                             self.route['iif'],
                             self.route['oif'],
                             self.route['target']))

    def __init__(self, route, *argv, **kwarg):
        self.route = route
        kwarg['iclass'] = rtmsg.metrics
        kwarg['check'] = False
        super(Metrics, self).__init__(*argv, **kwarg)
