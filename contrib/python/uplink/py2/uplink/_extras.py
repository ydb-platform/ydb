# Standard library imports
import inspect
import collections
import pkg_resources


_INSTALLERS = collections.OrderedDict()
_ENTRY_POINTS = collections.OrderedDict()


class plugin(object):
    _BASE_ENTRY_POINT_NAME = "uplink.plugins."

    def __init__(self, name, _entry_points=_ENTRY_POINTS):
        self._name = self._BASE_ENTRY_POINT_NAME + name
        self._entry_points = _entry_points

    def __call__(self, func):
        self._entry_points[self._name] = func
        return func


class installer(object):
    def __init__(self, base_cls, _installers=_INSTALLERS):
        self._base_cls = base_cls
        self._installers = _installers

    def __call__(self, func):
        self._installers[self._base_cls] = func
        return func


def load_entry_points(
    _entry_points=_ENTRY_POINTS,
    _iter_entry_points=pkg_resources.iter_entry_points,
):
    for name in _entry_points:
        plugins = {
            entry_point.name: entry_point.load()
            for entry_point in _iter_entry_points(name)
        }
        func = _entry_points[name]
        for value in plugins.values():
            func(value)


def install(installable, _installers=_INSTALLERS):
    cls = installable if inspect.isclass(installable) else type(installable)
    for base_cls in _installers:
        if issubclass(cls, base_cls):
            _installers[base_cls](installable)
            break
    else:
        raise TypeError("Failed to install: '%s'" % str(installable))

    return installable
