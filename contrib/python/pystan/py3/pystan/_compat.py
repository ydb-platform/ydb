# -*- coding: utf-8 -*-
"""
py2/py3 compatibility support drawn from jinja2

see http://lucumr.pocoo.org/2013/5/21/porting-to-python-3-redux/

"""

import sys

PY2 = sys.version_info[0] == 2
_identity = lambda x: x

if not PY2:
    unichr = chr
    range_type = range
    text_type = str
    string_types = (str,)

    implements_to_string = _identity

    ifilter = filter
    imap = map
    izip = zip

else:
    unichr = unichr
    text_type = unicode
    range_type = xrange
    string_types = (str, unicode)

    def implements_to_string(cls):
        cls.__unicode__ = cls.__str__
        cls.__str__ = lambda x: x.__unicode__().encode('utf-8')
        return cls

    from itertools import imap, izip, ifilter
