import threading
from pyroute2 import netns
from pyroute2.common import basestring
from pyroute2.ndb.objects import RTNL_Object
from pyroute2.netlink.rtnl.nsinfmsg import nsinfmsg


def load_nsinfmsg(schema, target, event):
    #
    # check if there is corresponding source
    #
    netns_path = event.get_attr('NSINFO_PATH')
    if netns_path is None:
        schema.log.debug('ignore %s %s' % (target, event))
        return
    if schema.ndb._auto_netns:
        if netns_path.find('/var/run/docker') > -1:
            source_name = 'docker/%s' % netns_path.split('/')[-1]
        else:
            source_name = 'netns/%s' % netns_path.split('/')[-1]
        if event['header'].get('type', 0) % 2:
            if source_name in schema.ndb.sources.cache:
                schema.ndb.sources.remove(source_name, code=108, sync=False)
        elif source_name not in schema.ndb.sources.cache:
            sync_event = None
            if schema.ndb._dbm_autoload and not schema.ndb._dbm_ready.is_set():
                sync_event = threading.Event()
                schema.ndb._dbm_autoload.add(sync_event)
                schema.log.debug('queued event %s' % sync_event)
            else:
                sync_event = None
            schema.log.debug('starting netns source %s' % source_name)
            schema.ndb.sources.async_add(target=source_name,
                                         netns=netns_path,
                                         persistent=False,
                                         event=sync_event)
    schema.load_netlink('netns', target, event)


schema = (nsinfmsg
          .sql_schema()
          .unique_index('NSINFO_PATH'))

init = {'specs': [['netns', schema]],
        'classes': [['netns', nsinfmsg]],
        'event_map': {nsinfmsg: [load_nsinfmsg]}}


class NetNS(RTNL_Object):

    table = 'netns'
    msg_class = nsinfmsg
    table_alias = 'n'
    api = 'netns'

    def __init__(self, *argv, **kwarg):
        kwarg['iclass'] = nsinfmsg
        self.event_map = {nsinfmsg: "load_rtnlmsg"}
        super(NetNS, self).__init__(*argv, **kwarg)

    @staticmethod
    def spec_normalize(spec):
        if isinstance(spec, basestring):
            ret = {'path': spec}
        else:
            ret = dict(spec)
        path = netns._get_netnspath(ret['path'])
        # on Python3 _get_netnspath() returns bytes, not str, so
        # we have to decode it here in order to avoid issues with
        # cache keys and DB inserts
        if hasattr(path, 'decode'):
            path = path.decode('utf-8')
        ret['path'] = path
        return ret

    def __setitem__(self, key, value):
        if self.state == 'system':
            raise ValueError('attempt to change a readonly object')
        if key == 'path':
            value = netns._get_netnspath(value)
        return super(NetNS, self).__setitem__(key, value)
