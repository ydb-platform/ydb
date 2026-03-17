# -*- coding: utf-8 -*-

#
# furl - URL manipulation made simple.
#
# Ansgar Grunseid
# grunseid.com
# grunseid@gmail.com
#
# License: Build Amazing Things (Unlicense)
#

from .furl import *  # noqa

# Import all variables in __version__.py without explicit imports.
from . import __version__
globals().update(dict((k, v) for k, v in __version__.__dict__.items()))
