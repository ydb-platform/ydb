###############################################################################
#
# Python 2/3 compatibility functions for XlsxWriter.
#
# Copyright (c), 2013-2021, John McNamara, jmcnamara@cpan.org
#

import sys
from decimal import Decimal
from fractions import Fraction

try:
    # For compatibility between Python 2 and 3.
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# Types to check in Python 2/3.
if sys.version_info[0] == 2:
    int_types = (int, long)
    num_types = (float, int, long, Decimal, Fraction)
    str_types = basestring
else:
    int_types = (int)
    num_types = (float, int, Decimal, Fraction)
    str_types = str


def force_unicode(string):
    """Return string as a native string"""
    if sys.version_info[0] == 2:
        if isinstance(string, unicode):
            return string.encode('utf-8')
    return string
