import warnings

from pyroute2 import netns
from pyroute2.common import basestring
from pyroute2.netlink.rtnl.nsinfmsg import nsinfmsg
from pyroute2.requests.netns import NetNSFieldFilter

from ..objects import AsyncObject


async def load_nsinfmsg(schema, sources, target, event):
    #
    # check if there is corresponding source
    #
    netns_path = event.get_attr('NSINFO_PATH')
    if netns_path is None:
        schema.log.debug('ignore %s %s' % (target, event))
        return
    if schema.config['auto_netns']:
        warnings.warn(
            'automatic netns sourcing is being refactored', DeprecationWarning
        )
    await schema.load_netlink('netns', sources, target, event)


schema = nsinfmsg.sql_schema().unique_index('NSINFO_PATH')

init = {
    'specs': [['netns', schema]],
    'classes': [['netns', nsinfmsg]],
    'event_map': {nsinfmsg: [load_nsinfmsg]},
}


class NetNS(AsyncObject):
    table = 'netns'
    msg_class = nsinfmsg
    table_alias = 'n'
    api = 'netns'
    field_filter = NetNSFieldFilter

    def __init__(self, *argv, **kwarg):
        kwarg['iclass'] = nsinfmsg
        self.event_map = {nsinfmsg: "load_rtnlmsg"}
        super(NetNS, self).__init__(*argv, **kwarg)

    @classmethod
    def spec_normalize(cls, spec):
        if isinstance(spec, basestring):
            spec = {'path': spec}
        path = netns._get_netnspath(spec['path'])
        # on Python3 _get_netnspath() returns bytes, not str, so
        # we have to decode it here in order to avoid issues with
        # cache keys and DB inserts
        if hasattr(path, 'decode'):
            path = path.decode('utf-8')
        spec['path'] = path
        return spec

    def __setitem__(self, key, value):
        if self.state == 'system':
            raise ValueError('attempt to change a readonly object')
        if key == 'path':
            value = netns._get_netnspath(value).decode('utf-8')
        return super(NetNS, self).__setitem__(key, value)
