'''

Using the global view
=====================

The `addresses` view provides access to all the addresses registered in the DB,
as well as methods to create and remove them::

    eth0 = ndb.interfaces['eth0']

    # create an address
    (ndb
     .addresses
     .create(address='10.0.0.1', prefixlen=24, index=eth0['index'])
     .commit())

    # remove it
    (ndb
     .addresses['10.0.0.1/24']
     .remove()
     .commit())

    # list addresses
    (ndb
     .addresses
     .summary())  # see also other view dump methods

Using ipaddr views
==================

Interfaces also provide address views as subsets of the global
address view::

    (ndb
     .interfaces['eth0']
     .ipaddr
     .summary())

It is possible use the same API as with the global address view::

    (ndb
     .interfaces['eth0']
     .ipaddr
     .create(address='10.0.0.1', prefixlen=24)  # index is implied
     .commit())

Using interface methods
=======================

Interfaces provide also simple methods to manage addresses::

    (ndb
     .interfaces['eth0']
     .del_ip('172.16.0.1/24')                   # remove an existing address
     .del_ip(family=AF_INET)                    # remove all IPv4 addresses
     .add_ip('10.0.0.1/24')                     # add a new IP address
     .add_ip(address='10.0.0.2', prefixlen=24)  # if you prefer keyword args
     .set('state', 'up')
     .commit())

Functions `add_ip()` and `del_ip()` return the interface object, so they
can be chained as in the example above, and the final `commit()` will
commit all the changes in the chain.

The keywords to `del_ip()` are the same object field names that may be used
in the selectors or report filters::

    (ndb
     .interfaces['eth0']
     .del_ip(prefixlen=25)    # remove all addresses with mask /25
     .commit())

A match function that may be passed to the `del_ip()` is the same as for
`addresses.dump().filter()`, and it gets a named tuple as the argument.
The fields are named in the same way as address objects fields. So if you
want to filter addresses by a pattern or the `prefixlen` field with a
match function, you may use::

    (ndb
     .interfaces['eth0']
     .del_ip(lambda x: x.address.startswith('192.168'))
     .commit())

    (ndb
     .interfaces['eth1']
     .del_ip(lambda x: x.prefixlen == 25)
     .commit())

An empty `del_ip()` removes all the IP addresses on the interface::

    (ndb
     .interfaces['eth0']
     .del_ip()                # flush all the IP:s
     .commit())

Accessing one address details
=============================

Access an address as a separate RTNL object::

    print(ndb.addresses['10.0.0.1/24'])

Please notice that address objects are read-only, you may not change them,
only remove old ones, and create new.
'''
from pyroute2.ndb.objects import RTNL_Object
from pyroute2.common import dqn2int
from pyroute2.common import basestring
from pyroute2.netlink.rtnl.ifaddrmsg import ifaddrmsg

ifaddr_spec = (ifaddrmsg
               .sql_schema()
               .unique_index('family',
                             'prefixlen',
                             'index',
                             'IFA_ADDRESS',
                             'IFA_LOCAL')
               .foreign_key('interfaces',
                            ('f_target', 'f_tflags', 'f_index'),
                            ('f_target', 'f_tflags', 'f_index')))

init = {'specs': [['addresses', ifaddr_spec]],
        'classes': [['addresses', ifaddrmsg]],
        'event_map': {ifaddrmsg: ['addresses']}}


def norm_mask(value):
    if isinstance(value, basestring):
        if '.' in value:
            value = dqn2int(value)
        value = int(value)
    return value


class Address(RTNL_Object):

    table = 'addresses'
    msg_class = ifaddrmsg
    fields_normalize = {'prefixlen': norm_mask}
    api = 'addr'

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
                  main.f_target, main.f_tflags,
                  intf.f_IFLA_IFNAME, main.f_IFA_ADDRESS, main.f_prefixlen
              FROM
                  addresses AS main
              INNER JOIN
                  interfaces AS intf
              ON
                  main.f_index = intf.f_index
                  AND main.f_target = intf.f_target
              '''
        yield ('target', 'tflags', 'ifname', 'address', 'prefixlen')
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
        kwarg['iclass'] = ifaddrmsg
        self.event_map = {ifaddrmsg: "load_rtnlmsg"}
        super(Address, self).__init__(*argv, **kwarg)

    @staticmethod
    def spec_normalize(spec):
        '''
        Address key normalization::

            { ... }        ->  { ... }
            "10.0.0.1/24"  ->  {"address": "10.0.0.1",
                                "prefixlen": 24}
        '''
        if isinstance(spec, dict):
            ret = spec
        else:
            ret = {}
        if isinstance(spec, basestring):
            addr_spec = spec.split('/')
            ret['address'] = addr_spec[0]
            if len(addr_spec) > 1:
                ret['prefixlen'] = addr_spec[1]
        return ret

    def key_repr(self):
        return '%s/%s %s/%s' % (self.get('target', ''),
                                self.get('label', self.get('index', '')),
                                self.get('local', self.get('address', '')),
                                self.get('prefixlen', ''))
