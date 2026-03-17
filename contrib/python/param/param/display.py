import weakref

_display_accessors = {}
_reactive_display_objs = weakref.WeakSet()

def register_display_accessor(name, accessor, force=False):
    if name in _display_accessors and not force:
        if _display_accessors[name].__module__ != accessor.__module__:
            raise KeyError(
                f'Display accessor {name!r} already registered. Override it '
                'by setting force=True or unregister the existing accessor first.'
            )
    _display_accessors[name] = accessor
    for fn in _reactive_display_objs:
        setattr(fn, name, accessor(fn))

def unregister_display_accessor(name):
    if name not in _display_accessors:
        raise KeyError('No such display accessor: {name!r}')
    del _display_accessors[name]
    for fn in _reactive_display_objs:
        delattr(fn, name)
