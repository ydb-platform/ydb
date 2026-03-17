# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import sys

from natsort.natsort import (
    as_ascii,
    as_utf8,
    decoder,
    humansorted,
    index_humansorted,
    index_natsorted,
    index_realsorted,
    natsort_key,
    natsort_keygen,
    natsorted,
    ns,
    numeric_regex_chooser,
    order_by_index,
    realsorted,
)
from natsort.utils import chain_functions

if float(sys.version[:3]) < 3:
    from natsort.natsort import natcmp

__version__ = "6.2.1"

__all__ = [
    "natsort_key",
    "natsort_keygen",
    "natsorted",
    "humansorted",
    "realsorted",
    "index_natsorted",
    "index_humansorted",
    "index_realsorted",
    "order_by_index",
    "decoder",
    "natcmp",
    "as_ascii",
    "as_utf8",
    "ns",
    "chain_functions",
    "numeric_regex_chooser",
]

# Add the ns keys to this namespace for convenience.
globals().update(ns._asdict())
