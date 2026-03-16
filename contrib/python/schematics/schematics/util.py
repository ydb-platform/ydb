# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import sys

from .compat import *

try:
    from collections.abc import Sequence  # PY3
except ImportError:
    from collections import Sequence  # PY2

if PY2:
    try:
        from thread import get_ident
    except ImportError:
        from dummy_thread import get_ident
else:
    try:
        from _thread import get_ident
    except ImportError:
        from _dummy_thread import get_ident

__all__ = ['get_ident', 'setdefault', 'Constant', 'listify',
    'get_all_subclasses', 'ImportStringError', 'import_string']


def setdefault(obj, attr, value, search_mro=False, overwrite_none=False):
    if search_mro:
        exists = hasattr(obj, attr)
    else:
        exists = attr in obj.__dict__
    if exists and overwrite_none:
        if getattr(obj, attr) is None:
            exists = False
    if exists:
        value = getattr(obj, attr)
    else:
        setattr(obj, attr, value)
    return value


class Constant(int):

    def __new__(cls, name, value):
        return int.__new__(cls, value)

    def __init__(self, name, value):
        self.name = name
        int.__init__(self)

    def __repr__(self):
        return self.name

    __str__ = __repr__


def listify(value):
    if isinstance(value, list):
        return value
    elif value is None:
        return []
    elif isinstance(value, string_type):
        return [value]
    elif isinstance(value, Sequence):
        return list(value)
    else:
        return [value]


def get_all_subclasses(cls):
    all_subclasses = []

    for subclass in cls.__subclasses__():
        all_subclasses.append(subclass)
        all_subclasses.extend(get_all_subclasses(subclass))

    return all_subclasses


class ImportStringError(ImportError):

    """Provides information about a failed :func:`import_string` attempt.

    Code taken from werzeug BSD license at https://github.com/pallets/werkzeug/blob/master/LICENSE
    """

    #: String in dotted notation that failed to be imported.
    import_name = None
    #: Wrapped exception.
    exception = None

    def __init__(self, import_name, exception):
        self.import_name = import_name
        self.exception = exception

        msg = (
            'import_string() failed for %r. Possible reasons are:\n\n'
            '- missing __init__.py in a package;\n'
            '- package or module path not included in sys.path;\n'
            '- duplicated package or module name taking precedence in '
            'sys.path;\n'
            '- missing module, class, function or variable;\n\n'
            'Debugged import:\n\n%s\n\n'
            'Original exception:\n\n%s: %s')

        name = ''
        tracked = []
        for part in import_name.replace(':', '.').split('.'):
            name += (name and '.') + part
            imported = import_string(name, silent=True)
            if imported:
                tracked.append((name, getattr(imported, '__file__', None)))
            else:
                track = ['- %r found in %r.' % (n, i) for n, i in tracked]
                track.append('- %r not found.' % name)
                msg = msg % (import_name, '\n'.join(track),
                             exception.__class__.__name__, str(exception))
                break

        ImportError.__init__(self, msg)

    def __repr__(self):
        return '<%s(%r, %r)>' % (self.__class__.__name__, self.import_name,
                                 self.exception)


def import_string(import_name, silent=False):
    """Imports an object based on a string.  This is useful if you want to
    use import paths as endpoints or something similar.  An import path can
    be specified either in dotted notation (``xml.sax.saxutils.escape``)
    or with a colon as object delimiter (``xml.sax.saxutils:escape``).

    If `silent` is True the return value will be `None` if the import fails.

    Code taken from werzeug BSD license at https://github.com/pallets/werkzeug/blob/master/LICENSE

    :param import_name: the dotted name for the object to import.
    :param silent: if set to `True` import errors are ignored and
                   `None` is returned instead.
    :return: imported object
    """
    # force the import name to automatically convert to strings
    # __import__ is not able to handle unicode strings in the fromlist
    # if the module is a package
    import_name = str(import_name).replace(':', '.')
    try:
        try:
            __import__(import_name)
        except ImportError:
            if '.' not in import_name:
                raise
        else:
            return sys.modules[import_name]

        module_name, obj_name = import_name.rsplit('.', 1)
        try:
            module = __import__(module_name, None, None, [obj_name])
        except ImportError:
            # support importing modules not yet set up by the parent module
            # (or package for that matter)
            module = import_string(module_name)

        try:
            return getattr(module, obj_name)
        except AttributeError as e:
            raise ImportError(e)

    except ImportError as e:
        if not silent:
            reraise(
                ImportStringError,
                ImportStringError(import_name, e),
                sys.exc_info()[2])


if PY2:
    # Python 2 names cannot be unicode
    __all__ = [n.encode('ascii') for n in __all__]
