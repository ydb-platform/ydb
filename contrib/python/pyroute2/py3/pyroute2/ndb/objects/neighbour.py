import errno

from pyroute2.common import basestring
from pyroute2.config import AF_BRIDGE
from pyroute2.netlink.rtnl.ndmsg import ndmsg
from pyroute2.requests.neighbour import NeighbourFieldFilter

from ..events import RescheduleException
from ..objects import AsyncObject


async def load_ndmsg(schema, sources, target, event):
    #
    # ignore events with ifindex == 0
    #
    if event['ifindex'] == 0:
        return
    #
    if event.get_attr('NDA_IFINDEX') is None:
        event['attrs'].append(('NDA_IFINDEX', event['ifindex']))
    #
    # AF_BRIDGE events
    #
    if event['family'] == AF_BRIDGE:
        #
        # bypass for now
        #
        try:
            await schema.load_netlink(
                'af_bridge_fdb', sources, target, event, propagate=True
            )
        except Exception:
            raise RescheduleException()

    else:
        await schema.load_netlink('neighbours', sources, target, event)


ndmsg_schema = (
    ndmsg.sql_schema()
    .unique_index('ifindex', 'NDA_DST', 'NDA_VLAN')
    .constraint('NDA_DST', "NOT NULL DEFAULT ''")
    .constraint('NDA_VLAN', "NOT NULL DEFAULT 0")
    .foreign_key(
        'interfaces',
        ('f_target', 'f_tflags', 'f_ifindex'),
        ('f_target', 'f_tflags', 'f_index'),
    )
)

brmsg_schema = (
    ndmsg.sql_schema()
    .unique_index('ifindex', 'flags', 'NDA_DST', 'NDA_LLADDR', 'NDA_VLAN')
    .constraint('NDA_LLADDR', "NOT NULL DEFAULT ''")
    .constraint('NDA_DST', "NOT NULL DEFAULT ''")
    .constraint('NDA_VLAN', "NOT NULL DEFAULT 0")
    .foreign_key(
        'interfaces',
        ('f_target', 'f_tflags', 'f_ifindex'),
        ('f_target', 'f_tflags', 'f_index'),
    )
)

init = {
    'specs': [['neighbours', ndmsg_schema], ['af_bridge_fdb', brmsg_schema]],
    'classes': [['neighbours', ndmsg], ['af_bridge_fdb', ndmsg]],
    'event_map': {ndmsg: [load_ndmsg]},
}


async def fallback_add(self, idx_req, req):
    async for msg in await self.sources[self['target']].api(self.api, 'dump'):
        await self.sources[self['target']].evq.put(msg)
    self.load_sql()


class Neighbour(AsyncObject):
    table = 'neighbours'
    msg_class = ndmsg
    field_filter = NeighbourFieldFilter
    api = 'neigh'

    @classmethod
    def _count(cls, view):
        if view.chain:
            return view.ndb.schema.fetchone(
                f'SELECT count(*) FROM {view.table} WHERE f_ifindex = ?',
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
                           main.f_ifindex = ?
                    '''
            values = [view.chain['target'], view.chain['index']]
        else:
            where = ''
            values = []
        return (where, values)

    @classmethod
    def summary(cls, view):
        req = f'''
              SELECT
                  main.f_target, main.f_tflags,
                  intf.f_IFLA_IFNAME, main.f_NDA_LLADDR, main.f_NDA_DST
              FROM
                  {cls.table} AS main
              INNER JOIN
                  interfaces AS intf
              ON
                  main.f_ifindex = intf.f_index
                  AND main.f_target = intf.f_target
              '''
        yield ('target', 'tflags', 'ifname', 'lladdr', 'dst')
        where, values = cls._dump_where(view)
        for record in view.ndb.schema.fetch(req + where, values):
            yield record

    def __init__(self, *argv, **kwarg):
        kwarg['iclass'] = ndmsg
        self.event_map = {ndmsg: "load_rtnlmsg"}
        super(Neighbour, self).__init__(*argv, **kwarg)
        self.fallback_for['add'][errno.EEXIST] = fallback_add

    def complete_key(self, key):
        if isinstance(key, dict):
            ret_key = key
        else:
            ret_key = {'target': self.ndb.config.localhost}

        if isinstance(key, basestring):
            ret_key['NDA_DST'] = key

        return super(Neighbour, self).complete_key(ret_key)

    def make_req(self, prime):
        req = super(Neighbour, self).make_req(prime)
        if 'vlan' in req and req['vlan'] == 0:
            req.pop('vlan')
        return req


class FDBRecord(Neighbour):
    table = 'af_bridge_fdb'
    msg_class = ndmsg
    api = 'fdb'

    @classmethod
    def summary(cls, view):
        for record in super(FDBRecord, cls).summary(view):
            yield record[:-1]

    def make_idx_req(self, prime):
        req = super(FDBRecord, self).make_req(prime)
        if 'NDA_VLAN' in req and req['NDA_VLAN'] == 0:
            req.pop('NDA_VLAN')
        return req
