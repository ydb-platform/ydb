from __future__ import unicode_literals

try:
    unicode
except NameError:
    unicode = str


def ensure_bytes(string):
    """Ensure a string is returned as a bytes object, encoded as utf8."""
    if isinstance(string, unicode):
        return string.encode("utf8")
    else:
        return string
