# -*- coding: utf-8 -*-
"""Module to generate chunked JSON replies for influxdb08."""

#
# Author: Adrian Sampson <adrian@radbox.org>
# Source: https://gist.github.com/sampsyo/920215
#

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json


def loads(s):
    """Generate a sequence of JSON values from a string."""
    _decoder = json.JSONDecoder()

    while s:
        s = s.strip()
        obj, pos = _decoder.raw_decode(s)
        if not pos:
            raise ValueError('no JSON object found at %i' % pos)
        yield obj
        s = s[pos:]
