'''
List interfaces
===============

List interface keys::

    with NDB(log='on') as ndb:
        for key in ndb.interfaces:
            print(key)

NDB views support some dict methods: `items()`, `values()`, `keys()`::

    with NDB(log='on') as ndb:
        for key, nic in ndb.interfaces.items():
            nic.set('state', 'up')
            nic.commit()

Get interface objects
=====================

The keys may be used as selectors to get interface objects::

    for key in ndb.interfaces:
        print(ndb.interfaces[key])

Also possible selector formats are `dict()` and simple string. The latter
means the interface name::

    eth0 = ndb.interfaces['eth0']

Dict selectors are necessary to get interfaces by other properties::

    wrk1_eth0 = ndb.interfaces[{'target': 'worker1.sample.com',
                                'ifname': 'eth0'}]

    wrk2_eth0 = ndb.interfaces[{'target': 'worker2.sample.com',
                                'address': '52:54:00:22:a1:b7'}]

Change nic properties
=====================

Changing MTU and MAC address::

    with ndb.interfaces['eth0'] as eth0:
        eth0['mtu'] = 1248
        eth0['address'] = '00:11:22:33:44:55'
    # --> <-- eth0.commit() is called by the context manager

One can change a property either using the assignment statement, or
using the `.set()` routine::

    # same code
    with ndb.interfaces['eth0'] as eth0:
        eth0.set('mtu', 1248)
        eth0.set('address', '00:11:22:33:44:55')

The `.set()` routine returns the object itself, that makes possible
chain calls::

    # same as above
    with ndb.interfaces['eth0'] as eth0:
        eth0.set('mtu', 1248).set('address', '00:11:22:33:44:55')

    # or
    with ndb.interfaces['eth0'] as eth0:
        (eth0
         .set('mtu', 1248)
         .set('address', '00:11:22:33:44:55'))

    # or without the context manager, call commit() explicitly
    (ndb
     .interfaces['eth0']
     .set('mtu', 1248)
     .set('address', '00:11:22:33:44:55')
     .commit())

Create virtual interfaces
=========================

Create a bridge and add a port, `eth0`::

    (ndb
     .interfaces
     .create(ifname='br0', kind='bridge')
     .commit())

    (ndb
     .interfaces['eth0']
     .set('master', ndb.interfaces['br0']['index'])
     .commit())

'''

import errno
import traceback
from pyroute2.config import AF_BRIDGE
from pyroute2.ndb.objects import RTNL_Object
from pyroute2.common import basestring
from pyroute2.netlink.rtnl.ifinfmsg import ifinfmsg
from pyroute2.netlink.rtnl.p2pmsg import p2pmsg
from pyroute2.netlink.exceptions import NetlinkError
from pyroute2.ndb.auth_manager import AuthManager
from pyroute2.ndb.auth_manager import check_auth


def load_ifinfmsg(schema, target, event):
    #
    # link goes down: flush all related routes
    #
    if not event['flags'] & 1:
        schema.execute('DELETE FROM routes WHERE '
                       'f_target = %s AND '
                       'f_RTA_OIF = %s OR f_RTA_IIF = %s'
                       % (schema.plch, schema.plch, schema.plch),
                       (target, event['index'], event['index']))
    #
    # ignore wireless updates
    #
    if event.get_attr('IFLA_WIRELESS'):
        return
    #
    # AF_BRIDGE events
    #
    if event['family'] == AF_BRIDGE:
        #
        schema.load_netlink('af_bridge_ifs', target, event)
        try:
            vlans = (event
                     .get_attr('IFLA_AF_SPEC')
                     .get_attrs('IFLA_BRIDGE_VLAN_INFO'))
        except AttributeError:
            # AttributeError: 'NoneType' object has no attribute 'get_attrs'
            # -- vlan filters not supported
            return

        # flush the old vlans info
        schema.execute('''
                       DELETE FROM af_bridge_vlans
                       WHERE
                           f_target = %s
                           AND f_index = %s
                       ''' % (schema.plch, schema.plch),
                       (target, event['index']))
        for v in vlans:
            v['index'] = event['index']
            v['header'] = {'type': event['header']['type']}
            schema.load_netlink('af_bridge_vlans', target, v)

        return

    schema.load_netlink('interfaces', target, event)
    #
    # load ifinfo, if exists
    #
    if not event['header'].get('type', 0) % 2:
        linkinfo = event.get_attr('IFLA_LINKINFO')
        if linkinfo is not None:
            iftype = linkinfo.get_attr('IFLA_INFO_KIND')
            table = 'ifinfo_%s' % iftype
            if iftype == 'gre':
                ifdata = linkinfo.get_attr('IFLA_INFO_DATA')
                local = ifdata.get_attr('IFLA_GRE_LOCAL')
                remote = ifdata.get_attr('IFLA_GRE_REMOTE')
                p2p = p2pmsg()
                p2p['index'] = event['index']
                p2p['family'] = 2
                p2p['attrs'] = [('P2P_LOCAL', local),
                                ('P2P_REMOTE', remote)]
                schema.load_netlink('p2p', target, p2p)
            elif iftype == 'veth':
                link = event.get_attr('IFLA_LINK')
                # for veth interfaces, IFLA_LINK points to
                # the peer -- but NOT in automatic updates
                if (not link) and \
                        (target in schema.ndb.sources.keys()):
                    schema.log.debug('reload veth %s' % event['index'])
                    update = (schema
                              .ndb
                              .sources[target]
                              .api('link', 'get', index=event['index']))
                    update = tuple(update)[0]
                    return schema.load_netlink('interfaces', target, update)

            if table in schema.spec:
                ifdata = linkinfo.get_attr('IFLA_INFO_DATA')
                ifdata['header'] = {}
                ifdata['index'] = event['index']
                schema.load_netlink(table, target, ifdata)


schema_ifinfmsg = (ifinfmsg
                   .sql_schema()
                   .unique_index('index'))

schema_brinfmsg = (ifinfmsg
                   .sql_schema()
                   .unique_index('index')
                   .foreign_key('interface',
                                ('f_target', 'f_tflags', 'f_index'),
                                ('f_target', 'f_tflags', 'f_index')))

schema_p2pmsg = (p2pmsg
                 .sql_schema()
                 .unique_index('index')
                 .foreign_key('interfaces',
                              ('f_target', 'f_tflags', 'f_index'),
                              ('f_target', 'f_tflags', 'f_index')))

schema_af_bridge_vlans = (ifinfmsg
                          .af_spec_bridge
                          .vlan_info
                          .sql_schema()
                          .push('index', 'INTEGER')
                          .unique_index('vid', 'index')
                          .foreign_key('af_bridge_ifs',
                                       ('f_target', 'f_tflags', 'f_index'),
                                       ('f_target', 'f_tflags', 'f_index')))

init = {'specs': [['interfaces', schema_ifinfmsg],
                  ['af_bridge_ifs', schema_ifinfmsg],
                  ['af_bridge_vlans', schema_af_bridge_vlans],
                  ['p2p', schema_p2pmsg]],
        'classes': [['interfaces', ifinfmsg],
                    ['af_bridge_ifs', ifinfmsg],
                    ['af_bridge_vlans', ifinfmsg.af_spec_bridge.vlan_info],
                    ['p2p', p2pmsg]],
        'event_map': {ifinfmsg: [load_ifinfmsg]}}

ifinfo_names = ('bridge',
                'bond',
                'vlan',
                'vxlan',
                'gre',
                'gretap',
                'ip6gre',
                'ip6gretap',
                'macvlan',
                'macvtap',
                'tun',
                'vrf',
                'vti',
                'vti6')
supported_ifinfo = {x: ifinfmsg.ifinfo.data_map[x] for x in ifinfo_names}
#
# load supported ifinfo
#
for (name, data) in supported_ifinfo.items():
    name = 'ifinfo_%s' % name
    init['classes'].append([name, data])
    schema = (data
              .sql_schema()
              .push('index', 'BIGINT')
              .unique_index('index')
              .foreign_key('interfaces',
                           ('f_target', 'f_tflags', 'f_index'),
                           ('f_target', 'f_tflags', 'f_index')))
    init['specs'].append([name, schema])


def _cmp_master(self, value):
    if self['master'] == value:
        return True
    elif self['master'] == 0 and value is None:
        dict.__setitem__(self, 'master', None)
        return True
    return False


def _norm_address(value):
    #
    # lower the case
    if not value.islower():
        value = value.lower()
    #
    # convert xxxx.xxxx.xxxx to xx:xx:xx:xx:xx:xx
    if len(value) == 14 and value[4] == value[9] == '.':
        value = ':'.join([':'.join((x[:2], x[2:])) for x in value.split('.')])

    return value


class Vlan(RTNL_Object):

    table = 'af_bridge_vlans'
    msg_class = ifinfmsg.af_spec_bridge.vlan_info
    api = 'vlan_filter'

    @classmethod
    def _dump_where(cls, view):
        if view.chain:
            plch = view.ndb.schema.plch
            where = '''
                    WHERE
                        main.f_target = %s AND
                        main.f_index = %s
                    ''' % (plch, plch)
            values = [view.chain['target'], view.chain['index']]
        else:
            where = ''
            values = []
        return (where, values)

    @classmethod
    def summary(cls, view):
        req = '''
              SELECT
                  main.f_target, main.f_tflags, main.f_vid,
                  intf.f_IFLA_IFNAME
              FROM
                  af_bridge_vlans AS main
              INNER JOIN
                  interfaces AS intf
              ON
                  main.f_index = intf.f_index
                  AND main.f_target = intf.f_target
              '''
        yield ('target', 'tflags', 'vid', 'ifname')
        where, values = cls._dump_where(view)
        for record in view.ndb.schema.fetch(req + where, values):
            yield record

    def __init__(self, *argv, **kwarg):
        kwarg['iclass'] = ifinfmsg.af_spec_bridge.vlan_info
        if 'auth_managers' not in kwarg or kwarg['auth_managers'] is None:
            kwarg['auth_managers'] = []
        log = argv[0].ndb.log.channel('vlan auth')
        kwarg['auth_managers'].append(AuthManager({'obj:modify': False}, log))
        super(Vlan, self).__init__(*argv, **kwarg)

    def make_req(self, prime):
        ret = {}
        if 'index' in self:
            ret['index'] = self['index']
        ret['vlan_info'] = {'vid': self['vid']}
        if 'flags' in self:
            ret['vlan_info']['flags'] = self['flags']
        return ret

    def make_idx_req(self, prime):
        return self.make_req(prime)


class Interface(RTNL_Object):

    table = 'interfaces'
    msg_class = ifinfmsg
    api = 'link'
    key_extra_fields = ['IFLA_IFNAME']
    resolve_fields = ['vxlan_link', 'link', 'master']
    fields_cmp = {'master': _cmp_master}
    fields_normalize = {'address': _norm_address}

    @classmethod
    def _dump_where(cls, view):
        if view.chain:
            plch = view.ndb.schema.plch
            where = '''
                    WHERE
                        f_target = %s AND
                        f_IFLA_MASTER = %s
                    ''' % (plch, plch)
            values = [view.chain['target'], view.chain['index']]
        else:
            where = 'WHERE f_index != 0'
            values = []
        return (where, values)

    @classmethod
    def summary(cls, view):
        req = '''
              SELECT
                  f_target, f_tflags, f_index,
                  f_IFLA_IFNAME, f_IFLA_ADDRESS,
                  f_flags, f_IFLA_INFO_KIND
              FROM
                  interfaces
              '''
        yield ('target', 'tflags', 'index',
               'ifname', 'address',
               'flags', 'kind')
        where, values = cls._dump_where(view)
        for record in view.ndb.schema.fetch(req + where, values):
            yield record

    def mark_tflags(self, mark):
        plch = (self.schema.plch, ) * 3
        self.schema.execute('''
                            UPDATE interfaces SET
                                f_tflags = %s
                            WHERE f_index = %s AND f_target = %s
                            ''' % plch, (mark, self['index'], self['target']))

    def __init__(self, *argv, **kwarg):
        kwarg['iclass'] = ifinfmsg
        self.event_map = {ifinfmsg: "load_rtnlmsg"}
        dict.__setitem__(self, 'flags', 0)
        dict.__setitem__(self, 'state', 'unknown')
        warnings = []
        if isinstance(argv[1], dict):
            if 'reuse' in argv[1]:
                warnings.append('ignore IPDB-specific `reuse` keyword')
                del argv[1]['reuse']
            if argv[1].get('create') and 'ifname' not in argv[1]:
                raise Exception('specify at least ifname')
            # type specific cases
            if argv[1].get('kind') == 'tuntap':
                # translate custom tuntap format into the native tun
                warnings.append('translated tuntap ifinfo into tun, no flags')
                argv[1]['kind'] = 'tun'
                if argv[1].get('mode') == 'tun':
                    argv[1]['tun_type'] = 1
                elif argv[1].get('mode') == 'tap':
                    argv[1]['tun_type'] = 2
                else:
                    raise TypeError('tun type error')
                del argv[1]['mode']
                if 'uid' in argv[1]:
                    argv[1]['tun_owner'] = argv[1].pop('uid')
                if 'gid' in argv[1]:
                    argv[1]['tun_owner'] = argv[1].pop('gid')
        super(Interface, self).__init__(*argv, **kwarg)
        for line in warnings:
            self.log.warning(line)

    @property
    def ipaddr(self):
        return self.view.ndb._get_view('addresses', chain=self)

    @property
    def ports(self):
        return self.view.ndb._get_view('interfaces', chain=self)

    @property
    def routes(self):
        return self.view.ndb._get_view('routes', chain=self)

    @property
    def neighbours(self):
        return self.view.ndb._get_view('neighbours', chain=self)

    @property
    def vlans(self):
        return self.view.ndb._get_view('vlans', chain=self)

    @property
    def context(self):
        ctx = {}
        if self.get('target'):
            ctx['target'] = self['target']
        if self.get('index'):
            ctx['index'] = self['index']
        return ctx

    @classmethod
    def compare_record(self, left, right):
        # specific compare
        if isinstance(right, basestring):
            return right == left['ifname'] or right == left['address']

    @check_auth('obj:modify')
    def add_vlan(self, spec):
        def do_add_vlan(self, spec):
            try:
                self.vlan.create(spec).apply()
                return []
            except Exception as e_s:
                e_s.trace = traceback.format_stack()
                return [e_s]
        self._apply_script.append((do_add_vlan, (self, spec), {}))
        return self

    @check_auth('obj:modify')
    def del_vlan(self, spec):
        def do_del_vlan(self, spec):
            try:
                ret = self.vlan[spec].remove().apply()
                return [ret.last_save]
            except Exception as e_s:
                e_s.trace = traceback.format_stack()
                return [e_s]
        self._apply_script.append((do_del_vlan, (self, spec), {}))
        return self

    @check_auth('obj:modify')
    def add_ip(self, spec=None, **kwarg):
        if spec is None and not kwarg:
            raise TypeError('ip spec is required')
        if kwarg:
            spec = dict(kwarg)

        def do_add_ip(self, spec):
            try:
                self.ipaddr.create(spec).apply()
                return []
            except Exception as e_s:
                e_s.trace = traceback.format_stack()
                return [e_s]
        self._apply_script.append((do_add_ip, (self, spec), {}))
        return self

    @check_auth('obj:modify')
    def del_ip(self, spec=None, **kwarg):
        if kwarg:
            spec = dict(kwarg)
        if spec is None and not kwarg:
            spec = {}

        def do_del_ip(self, match):
            ret = []
            if isinstance(match, basestring):
                specs = [match]
            elif callable(match):
                specs = self.ipaddr.dump().filter(match)
            else:
                specs = self.ipaddr.dump().filter(**match)
            for spec in specs:
                try:
                    ret.append(self.ipaddr.locate(spec).remove().apply())
                except KeyError:
                    pass
                except Exception as e_s:
                    e_s.trace = traceback.format_stack()
                    ret.append(e_s)
            if not ret:
                ret = [KeyError('no address records matched')]
            return ret

        self._apply_script.append((do_del_ip, (self, spec), {}))
        return self

    @check_auth('obj:modify')
    def add_port(self, spec):
        def do_add_port(self, spec):
            try:
                port = self.view[spec]
                assert port['target'] == self['target']
                port['master'] = self['index']
                port.apply()
                return [port.last_save]
            except Exception as e_s:
                e_s.trace = traceback.format_stack()
                return [e_s]
        self._apply_script.append((do_add_port, (self, spec), {}))
        return self

    @check_auth('obj:modify')
    def del_port(self, spec):
        def do_del_port(self, spec):
            try:
                port = self.view[spec]
                assert port['master'] == self['index']
                assert port['target'] == self['target']
                port['master'] = 0
                port.apply()
                return [port.last_save]
            except Exception as e_s:
                e_s.trace = traceback.format_stack()
                return [e_s]
        self._apply_script.append((do_del_port, (self, spec), {}))
        return self

    @check_auth('obj:modify')
    def __setitem__(self, key, value):
        if key == 'peer':
            dict.__setitem__(self, key, value)
        elif key == 'target' and self.state == 'invalid':
            dict.__setitem__(self, key, value)
        elif key == 'net_ns_fd' and self.state == 'invalid':
            dict.__setitem__(self, 'target', value)
        elif key == 'target' and \
                self.get('target') and \
                self['target'] != value:
            super(Interface, self).__setitem__('net_ns_fd', value)
        else:
            super(Interface, self).__setitem__(key, value)

    @staticmethod
    def spec_normalize(spec):
        '''
        Interface key normalization::

            { ... }  ->  { ... }
            "eth0"   ->  {"ifname": "eth0", ...}
            1        ->  {"index": 1, ...}

        '''
        if isinstance(spec, dict):
            ret = dict(spec)
        else:
            ret = {}
        if isinstance(spec, basestring):
            ret['ifname'] = spec
        elif isinstance(spec, int):
            ret['index'] = spec
        #
        # fix the master interface reference
        for key in ('vxlan_link', 'link', 'master'):
            if isinstance(ret.get(key), dict):
                ret[key] = ret[key]['index']

        return ret

    def complete_key(self, key):
        if isinstance(key, dict):
            ret_key = key
        else:
            ret_key = {'target': self.ndb.localhost}

        if isinstance(key, basestring):
            ret_key['ifname'] = key
        elif isinstance(key, int):
            ret_key['index'] = key

        return super(Interface, self).complete_key(ret_key)

    def snapshot(self, ctxid=None):
        # 1. make own snapshot
        snp = super(Interface, self).snapshot(ctxid=ctxid)
        # 2. collect dependencies and store in self.snapshot_deps
        for spec in (self
                     .ndb
                     .interfaces
                     .getmany({'IFLA_MASTER': self['index']})):
            # bridge ports
            link = type(self)(self.view,
                              spec,
                              auth_managers=self.auth_managers)
            snp.snapshot_deps.append((link, link.snapshot()))
        for spec in (self
                     .ndb
                     .interfaces
                     .getmany({'IFLA_LINK': self['index']})):
            # vlans & veth
            if self.get('link') != spec['index']:
                link = type(self)(self.view,
                                  spec,
                                  auth_managers=self.auth_managers)
                snp.snapshot_deps.append((link, link.snapshot()))
        # return the root node
        return snp

    def sync_req(self, prime):
        req = super(Interface, self).sync_req(prime)
        #
        # fix the master interface reference
        for key in ('vxlan_link', 'link', 'master'):
            if isinstance(req.get(key), dict):
                req[key] = req[key]['index']
            if isinstance(self.get(key), dict):
                self[key] = self[key]['index']
        return req

    def make_req(self, prime):
        req = super(Interface, self).make_req(prime)
        #
        # --> link('set', ...)
        if self.state == 'system':
            req['master'] = self['master']
        return req

    @check_auth('obj:modify')
    def apply(self, rollback=False, req_filter=None):
        # translate string link references into numbers
        for key in ('link', 'master'):
            if key in self and isinstance(self[key], basestring):
                self[key] = self.ndb.interfaces[self[key]]['index']
        setns = self.state.get() == 'setns'
        try:
            super(Interface, self).apply(rollback, req_filter)
        except NetlinkError as e:
            if e.code == 95 and \
                    self.get('master') is not None and \
                    self.get('master') > 0 and \
                    self.state == 'invalid':
                #
                # on some old kernels it is impossible to create
                # interfaces with master set; attempt to do it in
                # two steps
                def req_filter(req):
                    return dict([x for x in req.items()
                                 if not x[0].startswith('master')])
                self.apply(rollback, req_filter)
                self.apply(rollback)

            elif e.code == 95 and \
                    self.get('br_vlan_filtering') is not None and \
                    self.get('br_vlan_filtering') == 0:
                #
                # if vlan filtering is not enabled, then the parameter
                # is reported by netlink, but not accepted upon bridge
                # creation, so simply strip it
                def req_filter(req):
                    return dict([x for x in req.items()
                                 if not x[0].startswith('br_vlan_')])

                self.apply(rollback, req_filter)
            else:
                raise
        if setns:
            self.load_value('target', self['net_ns_fd'])
            dict.__setitem__(self, 'net_ns_fd', None)
            spec = self.load_sql()
            if spec:
                self.state.set('system')
        return self

    def hook_apply(self, method, **spec):
        if method == 'set':
            if self['kind'] == 'bridge':
                keys = filter(lambda x: x.startswith('br_'), self.changed)
                if keys:
                    req = {'index': self['index'],
                           'kind': 'bridge',
                           'family': AF_BRIDGE}
                    for key in keys:
                        req[key] = self[key]
                    (self
                     .sources[self['target']]
                     .api(self.api, method, **req))
                    update = (self
                              .sources[self['target']]
                              .api(self.api, 'get',
                                   **{'index': self['index']}))
                    self.ndb._event_queue.put(update)
        elif method == 'add':
            if self['kind'] == 'tun':
                self.load_sql()
                self.load_event.wait(0.1)
                if 'index' not in self:
                    raise NetlinkError(errno.EAGAIN)
                update = (self
                          .sources[self['target']]
                          .api(self.api, 'get', index=self['index']))
                self.ndb._event_queue.put(update)

    def load_sql(self, *argv, **kwarg):
        spec = super(Interface, self).load_sql(*argv, **kwarg)
        if spec:
            tname = 'ifinfo_%s' % self['kind']
            if tname in self.schema.compiled:
                names = self.schema.compiled[tname]['norm_names']
                spec = (self
                        .ndb
                        .schema
                        .fetchone('SELECT * from %s WHERE f_index = %s' %
                                  (tname, self.schema.plch),
                                  (self['index'], )))
                if spec:
                    self.update(dict(zip(names, spec)))
        return spec

    def load_rtnlmsg(self, *argv, **kwarg):
        super(Interface, self).load_rtnlmsg(*argv, **kwarg)

    def key_repr(self):
        return '%s/%s' % (self.get('target', ''),
                          self.get('ifname', self.get('index', '')))
