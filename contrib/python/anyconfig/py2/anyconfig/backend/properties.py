#
# Copyright (C) 2012 - 2017 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
r"""Java properties backend:

- Format to support: Java Properties file, e.g.
  http://docs.oracle.com/javase/1.5.0/docs/api/java/util/Properties.html
- Requirements: None (built-in)
- Development Status :: 4 - Beta
- Limitations:

  - Key and value separator of white spaces is not supported
  - Keys contain escaped white spaces is not supported

- Special options: None

Changelog:

.. versionchanged:: 0.7.0

   - Fix handling of empty values, pointed by @ajays20078
   - Fix handling of values contain strings start with '#' or '!' by
     @ajays20078

.. versionadded:: 0.2

   - Added native Java properties parser instead of a plugin utilizes
     pyjavaproperties module.
"""
from __future__ import absolute_import

import logging
import os
import re

import anyconfig.backend.base
import anyconfig.compat


LOGGER = logging.getLogger(__name__)
_COMMENT_MARKERS = ("#", "!")


def _parseline(line):
    """
    Parse a line of Java properties file.

    :param line:
        A string to parse, must not start with ' ', '#' or '!' (comment)
    :return: A tuple of (key, value), both key and value may be None

    >>> _parseline(" ")
    (None, '')
    >>> _parseline("aaa:")
    ('aaa', '')
    >>> _parseline(" aaa:")
    ('aaa', '')
    >>> _parseline("aaa")
    ('aaa', '')
    >>> _parseline("url = http://localhost")
    ('url', 'http://localhost')
    >>> _parseline("calendar.japanese.type: LocalGregorianCalendar")
    ('calendar.japanese.type', 'LocalGregorianCalendar')
    """
    pair = re.split(r"(?:\s+)?(?:(?<!\\)[=:])", line.strip(), 1)
    key = pair[0].rstrip()

    if len(pair) < 2:
        LOGGER.warning("Invalid line found: %s", line)
        return (key or None, '')

    return (key, pair[1].strip())


def _pre_process_line(line, comment_markers=_COMMENT_MARKERS):
    """
    Preprocess a line in properties; strip comments, etc.

    :param line:
        A string not starting w/ any white spaces and ending w/ line breaks.
        It may be empty. see also: :func:`load`.
    :param comment_markers: Comment markers, e.g. '#' (hash)

    >>> _pre_process_line('') is None
    True
    >>> s0 = "calendar.japanese.type: LocalGregorianCalendar"
    >>> _pre_process_line("# " + s0) is None
    True
    >>> _pre_process_line("! " + s0) is None
    True
    >>> _pre_process_line(s0 + "# comment")
    'calendar.japanese.type: LocalGregorianCalendar# comment'
    """
    if not line:
        return None

    if any(c in line for c in comment_markers):
        if line.startswith(comment_markers):
            return None

    return line


def unescape(in_s):
    """
    :param in_s: Input string
    """
    return re.sub(r"\\(.)", r"\1", in_s)


def _escape_char(in_c):
    """
    Escape some special characters in java .properties files.

    :param in_c: Input character

    >>> "\\:" == _escape_char(':')
    True
    >>> "\\=" == _escape_char('=')
    True
    >>> _escape_char('a')
    'a'
    """
    return '\\' + in_c if in_c in (':', '=', '\\') else in_c


def escape(in_s):
    """
    :param in_s: Input string
    """
    return ''.join(_escape_char(c) for c in in_s)


def load(stream, container=dict, comment_markers=_COMMENT_MARKERS):
    """
    Load and parse Java properties file given as a fiel or file-like object
    'stream'.

    :param stream: A file or file like object of Java properties files
    :param container:
        Factory function to create a dict-like object to store properties
    :param comment_markers: Comment markers, e.g. '#' (hash)
    :return: Dict-like object holding properties

    >>> to_strm = anyconfig.compat.StringIO
    >>> s0 = "calendar.japanese.type: LocalGregorianCalendar"
    >>> load(to_strm(''))
    {}
    >>> load(to_strm("# " + s0))
    {}
    >>> load(to_strm("! " + s0))
    {}
    >>> load(to_strm("calendar.japanese.type:"))
    {'calendar.japanese.type': ''}
    >>> load(to_strm(s0))
    {'calendar.japanese.type': 'LocalGregorianCalendar'}
    >>> load(to_strm(s0 + "# ..."))
    {'calendar.japanese.type': 'LocalGregorianCalendar# ...'}
    >>> s1 = r"key=a\\:b"
    >>> load(to_strm(s1))
    {'key': 'a:b'}
    >>> s2 = '''application/postscript: \\
    ...         x=Postscript File;y=.eps,.ps
    ... '''
    >>> load(to_strm(s2))
    {'application/postscript': 'x=Postscript File;y=.eps,.ps'}
    """
    ret = container()
    prev = ""

    for line in stream:
        line = _pre_process_line(prev + line.strip().rstrip(),
                                 comment_markers)
        # I don't think later case may happen but just in case.
        if line is None or not line:
            continue

        prev = ""  # re-initialize for later use.

        if line.endswith("\\"):
            prev += line.rstrip(" \\")
            continue

        (key, val) = _parseline(line)
        if key is None:
            LOGGER.warning("Failed to parse the line: %s", line)
            continue

        ret[key] = unescape(val)

    return ret


class Parser(anyconfig.backend.base.StreamParser):
    """
    Parser for Java properties files.
    """
    _cid = "properties"
    _type = "properties"
    _extensions = ["properties"]
    _ordered = True
    _dict_opts = ["ac_dict"]

    def load_from_stream(self, stream, container, **kwargs):
        """
        Load config from given file like object 'stream'.

        :param stream: A file or file like object of Java properties files
        :param container: callble to make a container object
        :param kwargs: optional keyword parameters (ignored)

        :return: Dict-like object holding config parameters
        """
        return load(stream, container=container)

    def dump_to_stream(self, cnf, stream, **kwargs):
        """
        Dump config 'cnf' to a file or file-like object 'stream'.

        :param cnf: Java properties config data to dump
        :param stream: Java properties file or file like object
        :param kwargs: backend-specific optional keyword parameters :: dict
        """
        for key, val in anyconfig.compat.iteritems(cnf):
            stream.write("%s = %s%s" % (key, escape(val), os.linesep))

# vim:sw=4:ts=4:et:
