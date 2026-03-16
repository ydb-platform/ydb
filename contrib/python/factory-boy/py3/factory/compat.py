# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.


"""Compatibility tools"""

import datetime
import sys

PY2 = (sys.version_info[0] == 2)

if PY2:  # pragma: no cover
    def is_string(obj):
        return isinstance(obj, (str, unicode))  # noqa

    def force_text(str_or_unicode):
        if isinstance(str_or_unicode, unicode):  # noqa
            return str_or_unicode
        return str_or_unicode.decode('utf-8')

else:  # pragma: no cover
    def is_string(obj):
        return isinstance(obj, str)

    def force_text(text):
        return text


try:  # pragma: no cover
    # Python >= 3.2
    UTC = datetime.timezone.utc
except AttributeError:  # pragma: no cover
    try:
        # Fallback to pytz
        from pytz import UTC
    except ImportError:

        # Ok, let's write our own.
        class _UTC(datetime.tzinfo):
            """The UTC tzinfo."""

            def utcoffset(self, dt):
                return datetime.timedelta(0)

            def tzname(self, dt):
                return "UTC"

            def dst(self, dt):
                return datetime.timedelta(0)

            def localize(self, dt):
                dt.astimezone(self)

        UTC = _UTC()
