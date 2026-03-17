import sys

from .calc import *
from .colls import *
from .tree import *
from .decorators import *
from .funcolls import *
from .funcs import *
from .seqs import *
from .types import *
from .strings import *
from .flow import *
from .objects import *
from .debug import *
from .primitives import *


# Setup __all__
modules = ('calc', 'colls', 'tree', 'decorators', 'funcolls', 'funcs', 'seqs', 'types',
           'strings', 'flow', 'objects', 'debug', 'primitives')
__all__ = lcat(sys.modules['funcy.' + m].__all__ for m in modules)
