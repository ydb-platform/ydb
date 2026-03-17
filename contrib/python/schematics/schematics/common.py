"""
Define constants and expose the compatibility overrides to all modules.
"""

from __future__ import unicode_literals, absolute_import

from .compat import *
from .compat import __all__ as compat_exports
from .util import Constant


NATIVE    = Constant('NATIVE',     0)
PRIMITIVE = Constant('PRIMITIVE',  1)

DROP      = Constant('DROP',       0)
NONEMPTY  = Constant('NONEMPTY',   1)
NOT_NONE  = Constant('NOT_NONE',   2)
DEFAULT   = Constant('DEFAULT',   10)
ALL       = Constant('ALL',       99)


__all__ = ['NATIVE', 'PRIMITIVE',
    'DROP', 'NONEMPTY', 'NOT_NONE', 'DEFAULT', 'ALL'] + compat_exports
if PY2:
    # Python 2 names cannot be unicode
    __all__ = [n.encode('ascii') for n in __all__]
