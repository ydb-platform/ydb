# -*- coding: utf-8 -*-
import functools
import sys

try:
    import simplejson as json
except ImportError:
    import json

PY2 = (sys.version_info[0] == 2)
PY3 = (sys.version_info[0] == 3)

if PY2:
    input = raw_input
    basestring = basestring
else:
    input = input
    basestring = str


def fix_pep_479(generator):
    """
    Python 3.7 breaks crossplane's lexer because of PEP 479
    Read more here: https://www.python.org/dev/peps/pep-0479/
    """
    @functools.wraps(generator)
    def _wrapped_generator(*args, **kwargs):
        try:
            for x in generator(*args, **kwargs):
                yield x
        except RuntimeError:
            return

    return _wrapped_generator
