from pyroute2.netlink.rt_files import TcClsFile
from pyroute2.netlink.rtnl import TC_H_ROOT
from pyroute2.netlink.rtnl.tcmsg import plugins as tc_plugins

from .common import IPRouteFilter


class TcRequestFilter:
    def transform_handle(self, key, context, handle):
        if isinstance(handle, str):
            if ':' not in handle:
                handle = TcClsFile().get_rt_id(handle)
            else:
                (major, minor) = [
                    int(x if x else '0', 16) for x in handle.split(':')
                ]
                handle = (major << 8 * 2) | minor
        return {key: handle}

    def set_handle(self, context, value):
        return self.transform_handle('handle', context, value)

    def set_root(self, context, value):
        ret = {}
        if value:
            ret['parent'] = 0xFFFFFFFF
            if 'handle' not in context:
                ret['handle'] = 0x10000
        return ret

    def set_target(self, context, value):
        return self.transform_handle('target', context, value)

    def set_parent(self, context, value):
        return self.transform_handle('parent', context, value)

    def set_default(self, context, value):
        return self.transform_handle('default', context, value)


class TcIPRouteFilter(IPRouteFilter):

    def finalize(self, context):

        if self.command.startswith('dump'):
            context.pop('kind', None)
            return

        if 'index' not in context:
            context['index'] = 0
        if 'handle' not in context:
            context['handle'] = 0

        # get & run the plugin
        if 'kind' in context:
            if context['kind'] in tc_plugins:
                plugin = tc_plugins[context['kind']]
                context['parent'] = context.get(
                    'parent', getattr(plugin, 'parent', 0)
                )
                if set(context.keys()) > set(('kind', 'index', 'handle')):
                    get_parameters = None
                    if self.command[-5:] == 'class':
                        get_parameters = getattr(
                            plugin, 'get_class_parameters', None
                        )
                    else:
                        get_parameters = getattr(
                            plugin, 'get_parameters', None
                        )
                    if get_parameters is not None:
                        context['options'] = get_parameters(dict(context))
                if hasattr(plugin, 'fix_request'):
                    plugin.fix_request(context)
            else:
                context['parent'] = TC_H_ROOT
