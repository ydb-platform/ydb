"""
pyRFC3339 parses and generates :RFC:`3339`-compliant timestamps using Python
:class:`datetime.datetime` objects.

>>> from pyrfc3339 import generate, parse
>>> from datetime import datetime, timezone
>>> generate(datetime.now(timezone.utc)) #doctest:+ELLIPSIS
'...T...Z'
>>> parse('2009-01-01T10:01:02Z')
datetime.datetime(2009, 1, 1, 10, 1, 2, tzinfo=datetime.timezone.utc)
>>> parse('2009-01-01T14:01:02-04:00')
datetime.datetime(2009, 1, 1, 14, 1, 2, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=72000)))

"""

from importlib.metadata import PackageNotFoundError, version

from .generator import generate
from .parser import parse

try:
    __version__ = version("pyrfc3339")
except PackageNotFoundError:
    pass

__all__ = ["generate", "parse"]
