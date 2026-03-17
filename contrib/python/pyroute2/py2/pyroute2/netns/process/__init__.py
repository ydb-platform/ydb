import fcntl
import types
import subprocess
from pyroute2.common import file


def _map_api(api, obj):
    for attr_name in dir(obj):
        attr = getattr(obj, attr_name)
        api[attr_name] = {'api': None}
        api[attr_name]['callable'] = hasattr(attr, '__call__')
        api[attr_name]['doc'] = attr.__doc__ \
            if hasattr(attr, '__doc__') else None


class MetaPopen(type):
    '''
    API definition for NSPopen.

    All this stuff is required to make `help()` function happy.
    '''
    def __init__(cls, *argv, **kwarg):
        super(MetaPopen, cls).__init__(*argv, **kwarg)
        # copy docstrings and create proxy slots
        cls.api = {}
        _map_api(cls.api, subprocess.Popen)
        for fname in ('stdin', 'stdout', 'stderr'):
            m = {}
            cls.api[fname] = {'callable': False,
                              'api': m}
            _map_api(m, file)
            for ename in ('fcntl', 'ioctl', 'flock', 'lockf'):
                m[ename] = {'api': None,
                            'callable': True,
                            'doc': getattr(fcntl, ename).__doc__}

    def __dir__(cls):
        return list(cls.api.keys()) + ['release']

    def __getattribute__(cls, key):
        try:
            return type.__getattribute__(cls, key)
        except AttributeError:
            attr = getattr(subprocess.Popen, key)
            if isinstance(attr, (types.MethodType, types.FunctionType)):
                def proxy(*argv, **kwarg):
                    return attr(*argv, **kwarg)
                proxy.__doc__ = attr.__doc__
                proxy.__objclass__ = cls
                return proxy
            else:
                return attr
