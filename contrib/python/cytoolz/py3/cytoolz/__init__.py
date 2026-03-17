from .itertoolz import *

from .functoolz import *

from .dicttoolz import *

from .recipes import *

from functools import partial, reduce

sorted = sorted
map = map
filter = filter

# Aliases
comp = compose

# Always-curried functions
flip = functoolz.flip = curry(functoolz.flip)
memoize = functoolz.memoize = curry(functoolz.memoize)

from . import curried  # sandbox

functoolz._sigs.update_signature_registry()

# What version of toolz does cytoolz implement?
__toolz_version__ = '1.1.0'


def __getattr__(name):
    if name == "__version__":
        from importlib.metadata import version

        rv = version("cytoolz")
        globals()[name] = rv
        return rv
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
