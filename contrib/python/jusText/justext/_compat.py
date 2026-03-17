# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

from sys import version_info


PY3 = version_info[0] == 3


if PY3:
    bytes = bytes
    unicode = str
else:
    bytes = str
    unicode = unicode
string_types = (bytes, unicode,)


if PY3:
    import urllib.request as urllib
    from urllib.error import URLError
else:
    import urllib2 as urllib
    URLError = urllib.URLError


try:
	from contextlib import ignored
except ImportError:
	from contextlib import contextmanager

	@contextmanager
	def ignored(*exceptions):
		try:
			yield
		except tuple(exceptions):
			pass


# note that cgi is depecrated and removed since 3.8
try:
    from html import escape
except ImportError:
    from cgi import escape
