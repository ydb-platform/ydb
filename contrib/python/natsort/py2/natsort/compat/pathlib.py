# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from pathlib import PurePath  # PurePath is the base object for Paths.
except ImportError:  # pragma: no cover
    PurePath = object  # To avoid NameErrors.
    has_pathlib = False
else:
    has_pathlib = True
