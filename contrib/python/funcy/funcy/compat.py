try:
    # Python 3
    from itertools import filterfalse
    filter, map, zip, range = filter, map, zip, range  # noqa
    basestring = (bytes, str)

    def lmap(f, *seqs):
        return list(map(f, *seqs))

    def lfilter(f, seq):
        return list(filter(f, seq))

except ImportError:
    # Python 2
    lmap, lfilter, range = map, filter, xrange  # noqa
    from itertools import (ifilter as filter, imap as map, izip as zip,  # noqa
                          ifilterfalse as filterfalse)
    basestring = basestring  # noqa


# collections.abc was added in Python 3.3
try:
    from collections.abc import Mapping, Set, Sequence, Iterable, Iterator, Hashable  # noqa
except ImportError:
    from collections import Mapping, Set, Sequence, Iterable, Iterator, Hashable  # noqa


import sys
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


# Taken from six and simplified
if PY3:
    exec("""def raise_from(value, from_value):
    try:
        raise value from from_value
    finally:
        value = None
""")
else:
    def raise_from(value, from_value):
        raise value
