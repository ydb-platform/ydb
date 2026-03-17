"""
Threading abstraction which allows for :mod:`threading2` use with a
transparent fallback to :mod:`threading` when it is not available.
Pyro doesn't use :mod:`threading` directly: it imports all
thread related items via this module instead. Code using Pyro can do
the same (but it is not required).

Pyro - Python Remote Objects.  Copyright by Irmen de Jong.
irmen@razorvine.net - http://www.razorvine.net/projects/Pyro
"""

from Pyro4 import config

if config.THREADING2:
    try:
        from threading2 import *
    except ImportError:
        from threading import *
else:
    from threading import *
