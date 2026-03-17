# -*- coding: utf-8 -*-

#
# furl - URL manipulation made simple.
#
# Ansgar Grunseid
# grunseid.com
# grunseid@gmail.com
#
# License: Build Amazing Things (Unlicense)
#

import re
import abc
import warnings
from copy import deepcopy
from posixpath import normpath

import six
from six.moves import urllib
from six.moves.urllib.parse import quote, unquote
try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

from .omdict1D import omdict1D
from .compat import string_types, UnicodeMixin
from .common import (
    callable_attr, is_iterable_but_not_string, absent as _absent)


# Map of common protocols, as suggested by the common protocols included in
# urllib/parse.py, to their default ports. Protocol scheme strings are
# lowercase.
#
# TODO(Ans): Is there a public map of schemes to their default ports? If not,
# create one? Best I (Ansgar) could find is
#
#   https://gist.github.com/mahmoud/2fe281a8daaff26cfe9c15d2c5bf5c8b
#
DEFAULT_PORTS = {
    'acap': 674,
    'afp': 548,
    'dict': 2628,
    'dns': 53,
    'ftp': 21,
    'git': 9418,
    'gopher': 70,
    'hdl': 2641,
    'http': 80,
    'https': 443,
    'imap': 143,
    'ipp': 631,
    'ipps': 631,
    'irc': 194,
    'ircs': 6697,
    'ldap': 389,
    'ldaps': 636,
    'mms': 1755,
    'msrp': 2855,
    'mtqp': 1038,
    'nfs': 111,
    'nntp': 119,
    'nntps': 563,
    'pop': 110,
    'prospero': 1525,
    'redis': 6379,
    'rsync': 873,
    'rtsp': 554,
    'rtsps': 322,
    'rtspu': 5005,
    'sftp': 22,
    'sip': 5060,
    'sips': 5061,
    'smb': 445,
    'snews': 563,
    'snmp': 161,
    'ssh': 22,
    'svn': 3690,
    'telnet': 23,
    'tftp': 69,
    'ventrilo': 3784,
    'vnc': 5900,
    'wais': 210,
    'ws': 80,
    'wss': 443,
    'xmpp': 5222,
}


def lget(lst, index, default=None):
    try:
        return lst[index]
    except IndexError:
        return default


def attemptstr(o):
    try:
        return str(o)
    except Exception:
        return o


def utf8(o, default=_absent):
    try:
        return o.encode('utf8')
    except Exception:
        return o if default is _absent else default


def non_string_iterable(o):
    return callable_attr(o, '__iter__') and not isinstance(o, string_types)


# TODO(grun): Support IDNA2008 via the third party idna module. See
# https://github.com/gruns/furl/issues/73#issuecomment-226549755.
def idna_encode(o):
    if callable_attr(o, 'encode'):
        return str(o.encode('idna').decode('utf8'))
    return o


def idna_decode(o):
    if callable_attr(utf8(o), 'decode'):
        return utf8(o).decode('idna')
    return o


def is_valid_port(port):
    port = str(port)
    if not port.isdigit() or not 0 < int(port) <= 65535:
        return False
    return True


def static_vars(**kwargs):
    def decorator(func):
        for key, value in six.iteritems(kwargs):
            setattr(func, key, value)
        return func
    return decorator


def create_quote_fn(safe_charset, quote_plus):
    def quote_fn(s, dont_quote):
        if dont_quote is True:
            safe = safe_charset
        elif dont_quote is False:
            safe = ''
        else:  # <dont_quote> is expected to be a string.
            safe = dont_quote

        # Prune duplicates and characters not in <safe_charset>.
        safe = ''.join(set(safe) & set(safe_charset))  # E.g. '?^#?' -> '?'.

        quoted = quote(s, safe)
        if quote_plus:
            quoted = quoted.replace('%20', '+')

        return quoted

    return quote_fn


#
# TODO(grun): Update some of the regex functions below to reflect the fact that
# the valid encoding of Path segments differs slightly from the valid encoding
# of Fragment Path segments. Similarly, the valid encodings of Query keys and
# values differ slightly from the valid encodings of Fragment Query keys and
# values.
#
# For example, '?' and '#' don't need to be encoded in Fragment Path segments
# but they must be encoded in Path segments. Similarly, '#' doesn't need to be
# encoded in Fragment Query keys and values, but must be encoded in Query keys
# and values.
#
# Perhaps merge them with URLPath, FragmentPath, URLQuery, and
# FragmentQuery when those new classes are created (see the TODO
# currently at the top of the source, 02/03/2012).
#

# RFC 3986 (https://www.ietf.org/rfc/rfc3986.txt)
#
#   unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
#
#   pct-encoded = "%" HEXDIG HEXDIG
#
#   sub-delims  = "!" / "$" / "&" / "'" / "(" / ")"
#                 / "*" / "+" / "," / ";" / "="
#
#   pchar       = unreserved / pct-encoded / sub-delims / ":" / "@"
#
#   === Path ===
#   segment     = *pchar
#
#   === Query ===
#   query       = *( pchar / "/" / "?" )
#
#   === Scheme ===
#   scheme      = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
#
PERCENT_REGEX = r'\%[a-fA-F\d][a-fA-F\d]'
INVALID_HOST_CHARS = '!@#$%^&\'\"*()+=:;/'


@static_vars(regex=re.compile(
    r'^([\w%s]|(%s))*$' % (re.escape('-.~:@!$&\'()*+,;='), PERCENT_REGEX)))
def is_valid_encoded_path_segment(segment):
    return is_valid_encoded_path_segment.regex.match(segment) is not None


@static_vars(regex=re.compile(
    r'^([\w%s]|(%s))*$' % (re.escape('-.~:@!$&\'()*+,;/?'), PERCENT_REGEX)))
def is_valid_encoded_query_key(key):
    return is_valid_encoded_query_key.regex.match(key) is not None


@static_vars(regex=re.compile(
    r'^([\w%s]|(%s))*$' % (re.escape('-.~:@!$&\'()*+,;/?='), PERCENT_REGEX)))
def is_valid_encoded_query_value(value):
    return is_valid_encoded_query_value.regex.match(value) is not None


@static_vars(regex=re.compile(r'[a-zA-Z][a-zA-Z\-\.\+]*'))
def is_valid_scheme(scheme):
    return is_valid_scheme.regex.match(scheme) is not None


@static_vars(regex=re.compile('[%s]' % re.escape(INVALID_HOST_CHARS)))
def is_valid_host(hostname):
    toks = hostname.split('.')
    if toks[-1] == '':  # Trailing '.' in a fully qualified domain name.
        toks.pop()

    for tok in toks:
        if is_valid_host.regex.search(tok) is not None:
            return False

    return '' not in toks  # Adjacent periods aren't allowed.


def get_scheme(url):
    if url.startswith(':'):
        return ''

    # Avoid incorrect scheme extraction with url.find(':') when other URL
    # components, like the path, query, fragment, etc, may have a colon in
    # them. For example, the URL 'a?query:', whose query has a ':' in it.
    no_fragment = url.split('#', 1)[0]
    no_query = no_fragment.split('?', 1)[0]
    no_path_or_netloc = no_query.split('/', 1)[0]
    scheme = url[:max(0, no_path_or_netloc.find(':'))] or None

    if scheme is not None and not is_valid_scheme(scheme):
        return None

    return scheme


def strip_scheme(url):
    scheme = get_scheme(url) or ''
    url = url[len(scheme):]
    if url.startswith(':'):
        url = url[1:]
    return url


def set_scheme(url, scheme):
    after_scheme = strip_scheme(url)
    if scheme is None:
        return after_scheme
    else:
        return '%s:%s' % (scheme, after_scheme)


# 'netloc' in Python parlance, 'authority' in RFC 3986 parlance.
def has_netloc(url):
    scheme = get_scheme(url)
    return url.startswith('//' if scheme is None else scheme + '://')


def urlsplit(url):
    """
    Parameters:
      url: URL string to split.
    Returns: urlparse.SplitResult tuple subclass, just like
      urlparse.urlsplit() returns, with fields (scheme, netloc, path,
      query, fragment, username, password, hostname, port). See
        http://docs.python.org/library/urlparse.html#urlparse.urlsplit
      for more details on urlsplit().
    """
    original_scheme = get_scheme(url)

    # urlsplit() parses URLs differently depending on whether or not the URL's
    # scheme is in any of
    #
    #   urllib.parse.uses_fragment
    #   urllib.parse.uses_netloc
    #   urllib.parse.uses_params
    #   urllib.parse.uses_query
    #   urllib.parse.uses_relative
    #
    # For consistent URL parsing, switch the URL's scheme to 'http', a scheme
    # in all of the aforementioned uses_* lists, and afterwards revert to the
    # original scheme (which may or may not be in some, or all, of the the
    # uses_* lists).
    if original_scheme is not None:
        url = set_scheme(url, 'http')

    scheme, netloc, path, query, fragment = urllib.parse.urlsplit(url)

    # Detect and preserve the '//' before the netloc, if present. E.g. preserve
    # URLs like 'http:', 'http://', and '///sup' correctly.
    after_scheme = strip_scheme(url)
    if after_scheme.startswith('//'):
        netloc = netloc or ''
    else:
        netloc = None

    scheme = original_scheme

    return urllib.parse.SplitResult(scheme, netloc, path, query, fragment)


def urljoin(base, url):
    """
    Parameters:
      base: Base URL to join with <url>.
      url: Relative or absolute URL to join with <base>.

    Returns: The resultant URL from joining <base> and <url>.
    """
    base_scheme = get_scheme(base) if has_netloc(base) else None
    url_scheme = get_scheme(url) if has_netloc(url) else None

    if base_scheme is not None:
        # For consistent URL joining, switch the base URL's scheme to
        # 'http'. urllib.parse.urljoin() behaves differently depending on the
        # scheme. E.g.
        #
        #   >>> urllib.parse.urljoin('http://google.com/', 'hi')
        #   'http://google.com/hi'
        #
        # vs
        #
        #   >>> urllib.parse.urljoin('asdf://google.com/', 'hi')
        #   'hi'
        root = set_scheme(base, 'http')
    else:
        root = base

    joined = urllib.parse.urljoin(root, url)

    new_scheme = url_scheme if url_scheme is not None else base_scheme
    if new_scheme is not None and has_netloc(joined):
        joined = set_scheme(joined, new_scheme)

    return joined


def join_path_segments(*args):
    """
    Join multiple lists of path segments together, intelligently
    handling path segments borders to preserve intended slashes of the
    final constructed path.

    This function is not encoding aware. It doesn't test for, or change,
    the encoding of path segments it is passed.

    Examples:
      join_path_segments(['a'], ['b']) == ['a','b']
      join_path_segments(['a',''], ['b']) == ['a','b']
      join_path_segments(['a'], ['','b']) == ['a','b']
      join_path_segments(['a',''], ['','b']) == ['a','','b']
      join_path_segments(['a','b'], ['c','d']) == ['a','b','c','d']

    Returns: A list containing the joined path segments.
    """
    finals = []

    for segments in args:
        if not segments or segments == ['']:
            continue
        elif not finals:
            finals.extend(segments)
        else:
            # Example #1: ['a',''] + ['b'] == ['a','b']
            # Example #2: ['a',''] + ['','b'] == ['a','','b']
            if finals[-1] == '' and (segments[0] != '' or len(segments) > 1):
                finals.pop(-1)
            # Example: ['a'] + ['','b'] == ['a','b']
            elif finals[-1] != '' and segments[0] == '' and len(segments) > 1:
                segments = segments[1:]
            finals.extend(segments)

    return finals


def remove_path_segments(segments, remove):
    """
    Removes the path segments of <remove> from the end of the path
    segments <segments>.

    Examples:
      # ('/a/b/c', 'b/c') -> '/a/'
      remove_path_segments(['','a','b','c'], ['b','c']) == ['','a','']
      # ('/a/b/c', '/b/c') -> '/a'
      remove_path_segments(['','a','b','c'], ['','b','c']) == ['','a']

    Returns: The list of all remaining path segments after the segments
    in <remove> have been removed from the end of <segments>. If no
    segments from <remove> were removed from <segments>, <segments> is
    returned unmodified.
    """
    # [''] means a '/', which is properly represented by ['', ''].
    if segments == ['']:
        segments.append('')
    if remove == ['']:
        remove.append('')

    ret = None
    if remove == segments:
        ret = []
    elif len(remove) > len(segments):
        ret = segments
    else:
        toremove = list(remove)

        if len(remove) > 1 and remove[0] == '':
            toremove.pop(0)

        if toremove and toremove == segments[-1 * len(toremove):]:
            ret = segments[:len(segments) - len(toremove)]
            if remove[0] != '' and ret:
                ret.append('')
        else:
            ret = segments

    return ret


def quacks_like_a_path_with_segments(obj):
    return (
        hasattr(obj, 'segments') and
        is_iterable_but_not_string(obj.segments))


class Path(object):

    """
    Represents a path comprised of zero or more path segments.

      http://tools.ietf.org/html/rfc3986#section-3.3

    Path parameters aren't supported.

    Attributes:
      _force_absolute: Function whos boolean return value specifies
        whether self.isabsolute should be forced to True or not. If
        _force_absolute(self) returns True, isabsolute is read only and
        raises an AttributeError if assigned to. If
        _force_absolute(self) returns False, isabsolute is mutable and
        can be set to True or False. URL paths use _force_absolute and
        return True if the netloc is non-empty (not equal to
        ''). Fragment paths are never read-only and their
        _force_absolute(self) always returns False.
      segments: List of zero or more path segments comprising this
        path. If the path string has a trailing '/', the last segment
        will be '' and self.isdir will be True and self.isfile will be
        False. An empty segment list represents an empty path, not '/'
        (though they have the same meaning).
      isabsolute: Boolean whether or not this is an absolute path or
        not. An absolute path starts with a '/'. self.isabsolute is
        False if the path is empty (self.segments == [] and str(path) ==
        '').
      strict: Boolean whether or not UserWarnings should be raised if
        improperly encoded path strings are provided to methods that
        take such strings, like load(), add(), set(), remove(), etc.
    """

    # From RFC 3986:
    #   segment       = *pchar
    #   pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
    #   unreserved    = ALPHA / DIGIT / "-" / "." / "_" / "~"
    #   sub-delims    = "!" / "$" / "&" / "'" / "(" / ")"
    #                       / "*" / "+" / "," / ";" / "="
    SAFE_SEGMENT_CHARS = ":@-._~!$&'()*+,;="

    def __init__(self, path='', force_absolute=lambda _: False, strict=False):
        self.segments = []

        self.strict = strict
        self._isabsolute = False
        self._force_absolute = force_absolute

        self.load(path)

    def load(self, path):
        """
        Load <path>, replacing any existing path. <path> can either be
        a Path instance, a list of segments, a path string to adopt.

        Returns: <self>.
        """
        if not path:
            segments = []
        elif quacks_like_a_path_with_segments(path):  # Path interface.
            segments = path.segments
        elif is_iterable_but_not_string(path):  # List interface.
            segments = path
        else:  # String interface.
            segments = self._segments_from_path(path)

        if self._force_absolute(self):
            self._isabsolute = True if segments else False
        else:
            self._isabsolute = (segments and segments[0] == '')

        if self.isabsolute and len(segments) > 1 and segments[0] == '':
            segments.pop(0)

        self.segments = segments

        return self

    def add(self, path):
        """
        Add <path> to the existing path. <path> can either be a Path instance,
        a list of segments, or a path string to append to the existing path.

        Returns: <self>.
        """
        if quacks_like_a_path_with_segments(path):  # Path interface.
            newsegments = path.segments
        elif is_iterable_but_not_string(path):  # List interface.
            newsegments = path
        else:  # String interface.
            newsegments = self._segments_from_path(path)

        # Preserve the opening '/' if one exists already (self.segments
        # == ['']).
        if self.segments == [''] and newsegments and newsegments[0] != '':
            newsegments.insert(0, '')

        segments = self.segments
        if self.isabsolute and self.segments and self.segments[0] != '':
            segments.insert(0, '')

        self.load(join_path_segments(segments, newsegments))

        return self

    def set(self, path):
        self.load(path)
        return self

    def remove(self, path):
        if path is True:
            self.load('')
        else:
            if is_iterable_but_not_string(path):  # List interface.
                segments = path
            else:  # String interface.
                segments = self._segments_from_path(path)
            base = ([''] if self.isabsolute else []) + self.segments
            self.load(remove_path_segments(base, segments))

        return self

    def normalize(self):
        """
        Normalize the path. Turn '//a/./b/../c//' into '/a/c/'.

        Returns: <self>.
        """
        if str(self):
            normalized = normpath(str(self)) + ('/' * self.isdir)
            if normalized.startswith('//'):  # http://bugs.python.org/636648
                normalized = '/' + normalized.lstrip('/')
            self.load(normalized)

        return self

    def asdict(self):
        return {
            'encoded': str(self),
            'isdir': self.isdir,
            'isfile': self.isfile,
            'segments': self.segments,
            'isabsolute': self.isabsolute,
            }

    @property
    def isabsolute(self):
        if self._force_absolute(self):
            return True
        return self._isabsolute

    @isabsolute.setter
    def isabsolute(self, isabsolute):
        """
        Raises: AttributeError if _force_absolute(self) returns True.
        """
        if self._force_absolute(self):
            s = ('Path.isabsolute is True and read-only for URLs with a netloc'
                 ' (a username, password, host, and/or port). A URL path must '
                 "start with a '/' to separate itself from a netloc.")
            raise AttributeError(s)
        self._isabsolute = isabsolute

    @property
    def isdir(self):
        """
        Returns: True if the path ends on a directory, False
        otherwise. If True, the last segment is '', representing the
        trailing '/' of the path.
        """
        return (self.segments == [] or
                (self.segments and self.segments[-1] == ''))

    @property
    def isfile(self):
        """
        Returns: True if the path ends on a file, False otherwise. If
        True, the last segment is not '', representing some file as the
        last segment of the path.
        """
        return not self.isdir

    def __truediv__(self, path):
        copy = deepcopy(self)
        return copy.add(path)

    def __eq__(self, other):
        return str(self) == str(other)

    def __ne__(self, other):
        return not self == other

    def __bool__(self):
        return len(self.segments) > 0
    __nonzero__ = __bool__

    def __str__(self):
        segments = list(self.segments)
        if self.isabsolute:
            if not segments:
                segments = ['', '']
            else:
                segments.insert(0, '')
        return self._path_from_segments(segments)

    def __repr__(self):
        return "%s('%s')" % (self.__class__.__name__, str(self))

    def _segments_from_path(self, path):
        """
        Returns: The list of path segments from the path string <path>.

        Raises: UserWarning if <path> is an improperly encoded path
        string and self.strict is True.

        TODO(grun): Accept both list values and string values and
        refactor the list vs string interface testing to this common
        method.
        """
        segments = []
        for segment in path.split('/'):
            if not is_valid_encoded_path_segment(segment):
                segment = quote(utf8(segment))
                if self.strict:
                    s = ("Improperly encoded path string received: '%s'. "
                         "Proceeding, but did you mean '%s'?" %
                         (path, self._path_from_segments(segments)))
                    warnings.warn(s, UserWarning)
            segments.append(utf8(segment))
        del segment

        # In Python 3, utf8() returns Bytes objects that must be decoded into
        # strings before they can be passed to unquote(). In Python 2, utf8()
        # returns strings that can be passed directly to urllib.unquote().
        segments = [
            segment.decode('utf8')
            if isinstance(segment, bytes) and not isinstance(segment, str)
            else segment for segment in segments]

        return [unquote(segment) for segment in segments]

    def _path_from_segments(self, segments):
        """
        Combine the provided path segments <segments> into a path string. Path
        segments in <segments> will be quoted.

        Returns: A path string with quoted path segments.
        """
        segments = [
            quote(utf8(attemptstr(segment)), self.SAFE_SEGMENT_CHARS)
            for segment in segments]
        return '/'.join(segments)


@six.add_metaclass(abc.ABCMeta)
class PathCompositionInterface(object):

    """
    Abstract class interface for a parent class that contains a Path.
    """

    def __init__(self, strict=False):
        """
        Params:
          force_absolute: See Path._force_absolute.

        Assignments to <self> in __init__() must be added to
        __setattr__() below.
        """
        self._path = Path(force_absolute=self._force_absolute, strict=strict)

    @property
    def path(self):
        return self._path

    @property
    def pathstr(self):
        """This method is deprecated. Use str(furl.path) instead."""
        s = ('furl.pathstr is deprecated. Use str(furl.path) instead. There '
             'should be one, and preferably only one, obvious way to serialize'
             ' a Path object to a string.')
        warnings.warn(s, DeprecationWarning)
        return str(self._path)

    @abc.abstractmethod
    def _force_absolute(self, path):
        """
        Subclass me.
        """
        pass

    def __setattr__(self, attr, value):
        """
        Returns: True if this attribute is handled and set here, False
        otherwise.
        """
        if attr == '_path':
            self.__dict__[attr] = value
            return True
        elif attr == 'path':
            self._path.load(value)
            return True
        return False


@six.add_metaclass(abc.ABCMeta)
class URLPathCompositionInterface(PathCompositionInterface):

    """
    Abstract class interface for a parent class that contains a URL
    Path.

    A URL path's isabsolute attribute is absolute and read-only if a
    netloc is defined. A path cannot start without '/' if there's a
    netloc. For example, the URL 'http://google.coma/path' makes no
    sense. It should be 'http://google.com/a/path'.

    A URL path's isabsolute attribute is mutable if there's no
    netloc. The scheme doesn't matter. For example, the isabsolute
    attribute of the URL path in 'mailto:user@host.com', with scheme
    'mailto' and path 'user@host.com', is mutable because there is no
    netloc. See

      http://en.wikipedia.org/wiki/URI_scheme#Examples
    """

    def __init__(self, strict=False):
        PathCompositionInterface.__init__(self, strict=strict)

    def _force_absolute(self, path):
        return bool(path) and self.netloc


@six.add_metaclass(abc.ABCMeta)
class FragmentPathCompositionInterface(PathCompositionInterface):

    """
    Abstract class interface for a parent class that contains a Fragment
    Path.

    Fragment Paths they be set to absolute (self.isabsolute = True) or
    not absolute (self.isabsolute = False).
    """

    def __init__(self, strict=False):
        PathCompositionInterface.__init__(self, strict=strict)

    def _force_absolute(self, path):
        return False


class Query(object):

    """
    Represents a URL query comprised of zero or more unique parameters
    and their respective values.

      http://tools.ietf.org/html/rfc3986#section-3.4


    All interaction with Query.params is done with unquoted strings. So

      f.query.params['a'] = 'a%5E'

    means the intended value for 'a' is 'a%5E', not 'a^'.


    Query.params is implemented as an omdict1D object - a one
    dimensional ordered multivalue dictionary. This provides support for
    repeated URL parameters, like 'a=1&a=2'. omdict1D is a subclass of
    omdict, an ordered multivalue dictionary. Documentation for omdict
    can be found here

      https://github.com/gruns/orderedmultidict

    The one dimensional aspect of omdict1D means that a list of values
    is interpreted as multiple values, not a single value which is
    itself a list of values. This is a reasonable distinction to make
    because URL query parameters are one dimensional: query parameter
    values cannot themselves be composed of sub-values.

    So what does this mean? This means we can safely interpret

      f = furl('http://www.google.com')
      f.query.params['arg'] = ['one', 'two', 'three']

    as three different values for 'arg': 'one', 'two', and 'three',
    instead of a single value which is itself some serialization of the
    python list ['one', 'two', 'three']. Thus, the result of the above
    will be

      f.query.allitems() == [
        ('arg','one'), ('arg','two'), ('arg','three')]

    and not

      f.query.allitems() == [('arg', ['one', 'two', 'three'])]

    The latter doesn't make sense because query parameter values cannot
    be composed of sub-values. So finally

      str(f.query) == 'arg=one&arg=two&arg=three'


    Additionally, while the set of allowed characters in URL queries is
    defined in RFC 3986 section 3.4, the format for encoding key=value
    pairs within the query is not. In turn, the parsing of encoded
    key=value query pairs differs between implementations.

    As a compromise to support equal signs in both key=value pair
    encoded queries, like

      https://www.google.com?a=1&b=2

    and non-key=value pair encoded queries, like

      https://www.google.com?===3===

    equal signs are percent encoded in key=value pairs where the key is
    non-empty, e.g.

      https://www.google.com?equal-sign=%3D

    but not encoded in key=value pairs where the key is empty, e.g.

      https://www.google.com?===equal=sign===

    This presents a reasonable compromise to accurately reproduce
    non-key=value queries with equal signs while also still percent
    encoding equal signs in key=value pair encoded queries, as
    expected. See

      https://github.com/gruns/furl/issues/99

    for more details.

    Attributes:
      params: Ordered multivalue dictionary of query parameter key:value
        pairs. Parameters in self.params are maintained URL decoded,
        e.g. 'a b' not 'a+b'.
      strict: Boolean whether or not UserWarnings should be raised if
        improperly encoded query strings are provided to methods that
        take such strings, like load(), add(), set(), remove(), etc.
    """

    # From RFC 3986:
    #   query       = *( pchar / "/" / "?" )
    #   pchar       = unreserved / pct-encoded / sub-delims / ":" / "@"
    #   unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
    #   sub-delims  = "!" / "$" / "&" / "'" / "(" / ")"
    #                     / "*" / "+" / "," / ";" / "="
    SAFE_KEY_CHARS = "/?:@-._~!$'()*+,;"
    SAFE_VALUE_CHARS = SAFE_KEY_CHARS + '='

    def __init__(self, query='', strict=False):
        self.strict = strict

        self._params = omdict1D()

        self.load(query)

    def load(self, query):
        items = self._items(query)
        self.params.load(items)
        return self

    def add(self, args):
        for param, value in self._items(args):
            self.params.add(param, value)
        return self

    def set(self, mapping):
        """
        Adopt all mappings in <mapping>, replacing any existing mappings
        with the same key. If a key has multiple values in <mapping>,
        they are all adopted.

        Examples:
          Query({1:1}).set([(1,None),(2,2)]).params.allitems()
            == [(1,None),(2,2)]
          Query({1:None,2:None}).set([(1,1),(2,2),(1,11)]).params.allitems()
            == [(1,1),(2,2),(1,11)]
          Query({1:None}).set([(1,[1,11,111])]).params.allitems()
            == [(1,1),(1,11),(1,111)]

        Returns: <self>.
        """
        self.params.updateall(mapping)
        return self

    def remove(self, query):
        if query is True:
            self.load('')
            return self

        # Single key to remove.
        items = [query]
        # Dictionary or multivalue dictionary of items to remove.
        if callable_attr(query, 'items'):
            items = self._items(query)
        # List of keys or items to remove.
        elif non_string_iterable(query):
            items = query

        for item in items:
            if non_string_iterable(item) and len(item) == 2:
                key, value = item
                self.params.popvalue(key, value, None)
            else:
                key = item
                self.params.pop(key, None)

        return self

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, params):
        items = self._items(params)

        self._params.clear()
        for key, value in items:
            self._params.add(key, value)

    def encode(self, delimiter='&', quote_plus=True, dont_quote='',
               delimeter=_absent):
        """
        Examples:

          Query('a=a&b=#').encode() == 'a=a&b=%23'
          Query('a=a&b=#').encode(';') == 'a=a;b=%23'
          Query('a+b=c@d').encode(dont_quote='@') == 'a+b=c@d'
          Query('a+b=c@d').encode(quote_plus=False) == 'a%20b=c%40d'

        Until furl v0.4.6, the 'delimiter' argument was incorrectly
        spelled 'delimeter'. For backwards compatibility, accept both
        the correct 'delimiter' and the old, misspelled 'delimeter'.

        Keys and values are encoded application/x-www-form-urlencoded if
        <quote_plus> is True, percent-encoded otherwise.

        <dont_quote> exempts valid query characters from being
        percent-encoded, either in their entirety with dont_quote=True,
        or selectively with dont_quote=<string>, like
        dont_quote='/?@_'. Invalid query characters -- those not in
        self.SAFE_KEY_CHARS, like '#' and '^' -- are always encoded,
        even if included in <dont_quote>. For example:

          Query('#=^').encode(dont_quote='#^') == '%23=%5E'.

        Returns: A URL encoded query string using <delimiter> as the
        delimiter separating key:value pairs. The most common and
        default delimiter is '&', but ';' can also be specified. ';' is
        W3C recommended.
        """
        if delimeter is not _absent:
            delimiter = delimeter

        quote_key = create_quote_fn(self.SAFE_KEY_CHARS, quote_plus)
        quote_value = create_quote_fn(self.SAFE_VALUE_CHARS, quote_plus)

        pairs = []
        for key, value in self.params.iterallitems():
            utf8key = utf8(key, utf8(attemptstr(key)))
            quoted_key = quote_key(utf8key, dont_quote)

            if value is None:  # Example: http://sprop.su/?key.
                pair = quoted_key
            else:  # Example: http://sprop.su/?key=value.
                utf8value = utf8(value, utf8(attemptstr(value)))
                quoted_value = quote_value(utf8value, dont_quote)

                if not quoted_key:  # Unquote '=' to allow queries like '?==='.
                    quoted_value = quoted_value.replace('%3D', '=')

                pair = '%s=%s' % (quoted_key, quoted_value)

            pairs.append(pair)

        query = delimiter.join(pairs)

        return query

    def asdict(self):
        return {
            'encoded': str(self),
            'params': self.params.allitems(),
            }

    def __eq__(self, other):
        return str(self) == str(other)

    def __ne__(self, other):
        return not self == other

    def __bool__(self):
        return len(self.params) > 0
    __nonzero__ = __bool__

    def __str__(self):
        return self.encode()

    def __repr__(self):
        return "%s('%s')" % (self.__class__.__name__, str(self))

    def _items(self, items):
        """
        Extract and return the key:value items from various
        containers. Some containers that could hold key:value items are

          - List of (key,value) tuples.
          - Dictionaries of key:value items.
          - Multivalue dictionary of key:value items, with potentially
            repeated keys.
          - Query string with encoded params and values.

        Keys and values are passed through unmodified unless they were
        passed in within an encoded query string, like
        'a=a%20a&b=b'. Keys and values passed in within an encoded query
        string are unquoted by urlparse.parse_qsl(), which uses
        urllib.unquote_plus() internally.

        Returns: List of items as (key, value) tuples. Keys and values
        are passed through unmodified unless they were passed in as part
        of an encoded query string, in which case the final keys and
        values that are returned will be unquoted.

        Raises: UserWarning if <path> is an improperly encoded path
        string and self.strict is True.
        """
        if not items:
            items = []
        # Multivalue Dictionary-like interface. e.g. {'a':1, 'a':2,
        # 'b':2}
        elif callable_attr(items, 'allitems'):
            items = list(items.allitems())
        elif callable_attr(items, 'iterallitems'):
            items = list(items.iterallitems())
        # Dictionary-like interface. e.g. {'a':1, 'b':2, 'c':3}
        elif callable_attr(items, 'items'):
            items = list(items.items())
        elif callable_attr(items, 'iteritems'):
            items = list(items.iteritems())
        # Encoded query string. e.g. 'a=1&b=2&c=3'
        elif isinstance(items, six.string_types):
            items = self._extract_items_from_querystr(items)
        # Default to list of key:value items interface. e.g. [('a','1'),
        # ('b','2')]
        else:
            items = list(items)

        return items

    def _extract_items_from_querystr(self, querystr):
        items = []

        pairstrs = querystr.split('&')
        pairs = [item.split('=', 1) for item in pairstrs]
        pairs = [(p[0], lget(p, 1, '')) for p in pairs]  # Pad with value ''.

        for pairstr, (key, value) in six.moves.zip(pairstrs, pairs):
            valid_key = is_valid_encoded_query_key(key)
            valid_value = is_valid_encoded_query_value(value)
            if self.strict and (not valid_key or not valid_value):
                msg = (
                    "Incorrectly percent encoded query string received: '%s'. "
                    "Proceeding, but did you mean '%s'?" %
                    (querystr, urllib.parse.urlencode(pairs)))
                warnings.warn(msg, UserWarning)

            key_decoded = unquote(key.replace('+', ' '))
            # Empty value without a '=', e.g. '?sup'.
            if key == pairstr:
                value_decoded = None
            else:
                value_decoded = unquote(value.replace('+', ' '))

            items.append((key_decoded, value_decoded))

        return items


@six.add_metaclass(abc.ABCMeta)
class QueryCompositionInterface(object):

    """
    Abstract class interface for a parent class that contains a Query.
    """

    def __init__(self, strict=False):
        self._query = Query(strict=strict)

    @property
    def query(self):
        return self._query

    @property
    def querystr(self):
        """This method is deprecated. Use str(furl.query) instead."""
        s = ('furl.querystr is deprecated. Use str(furl.query) instead. There '
             'should be one, and preferably only one, obvious way to serialize'
             ' a Query object to a string.')
        warnings.warn(s, DeprecationWarning)
        return str(self._query)

    @property
    def args(self):
        """
        Shortcut method to access the query parameters, self._query.params.
        """
        return self._query.params

    def __setattr__(self, attr, value):
        """
        Returns: True if this attribute is handled and set here, False
        otherwise.
        """
        if attr == 'args' or attr == 'query':
            self._query.load(value)
            return True
        return False


class Fragment(FragmentPathCompositionInterface, QueryCompositionInterface):

    """
    Represents a URL fragment, comprised internally of a Path and Query
    optionally separated by a '?' character.

      http://tools.ietf.org/html/rfc3986#section-3.5

    Attributes:
      path: Path object from FragmentPathCompositionInterface.
      query: Query object from QueryCompositionInterface.
      separator: Boolean whether or not a '?' separator should be
        included in the string representation of this fragment. When
        False, a '?' character will not separate the fragment path from
        the fragment query in the fragment string. This is useful to
        build fragments like '#!arg1=val1&arg2=val2', where no
        separating '?' is desired.
    """

    def __init__(self, fragment='', strict=False):
        FragmentPathCompositionInterface.__init__(self, strict=strict)
        QueryCompositionInterface.__init__(self, strict=strict)
        self.strict = strict
        self.separator = True

        self.load(fragment)

    def load(self, fragment):
        self.path.load('')
        self.query.load('')

        if fragment is None:
            fragment = ''

        toks = fragment.split('?', 1)
        if len(toks) == 0:
            self._path.load('')
            self._query.load('')
        elif len(toks) == 1:
            # Does this fragment look like a path or a query? Default to
            # path.
            if '=' in fragment:  # Query example: '#woofs=dogs'.
                self._query.load(fragment)
            else:  # Path example: '#supinthisthread'.
                self._path.load(fragment)
        else:
            # Does toks[1] actually look like a query? Like 'a=a' or
            # 'a=' or '=a'?
            if '=' in toks[1]:
                self._path.load(toks[0])
                self._query.load(toks[1])
            # If toks[1] doesn't look like a query, the user probably
            # provided a fragment string like 'a?b?' that was intended
            # to be adopted as-is, not a two part fragment with path 'a'
            # and query 'b?'.
            else:
                self._path.load(fragment)

    def add(self, path=_absent, args=_absent):
        if path is not _absent:
            self.path.add(path)
        if args is not _absent:
            self.query.add(args)

        return self

    def set(self, path=_absent, args=_absent, separator=_absent):
        if path is not _absent:
            self.path.load(path)
        if args is not _absent:
            self.query.load(args)
        if separator is True or separator is False:
            self.separator = separator

        return self

    def remove(self, fragment=_absent, path=_absent, args=_absent):
        if fragment is True:
            self.load('')
        if path is not _absent:
            self.path.remove(path)
        if args is not _absent:
            self.query.remove(args)

        return self

    def asdict(self):
        return {
            'encoded': str(self),
            'separator': self.separator,
            'path': self.path.asdict(),
            'query': self.query.asdict(),
            }

    def __eq__(self, other):
        return str(self) == str(other)

    def __ne__(self, other):
        return not self == other

    def __setattr__(self, attr, value):
        if (not PathCompositionInterface.__setattr__(self, attr, value) and
                not QueryCompositionInterface.__setattr__(self, attr, value)):
            object.__setattr__(self, attr, value)

    def __bool__(self):
        return bool(self.path) or bool(self.query)
    __nonzero__ = __bool__

    def __str__(self):
        path, query = str(self._path), str(self._query)

        # If there is no query or self.separator is False, decode all
        # '?' characters in the path from their percent encoded form
        # '%3F' to '?'. This allows for fragment strings containg '?'s,
        # like '#dog?machine?yes'.
        if path and (not query or not self.separator):
            path = path.replace('%3F', '?')

        separator = '?' if path and query and self.separator else ''

        return path + separator + query

    def __repr__(self):
        return "%s('%s')" % (self.__class__.__name__, str(self))


@six.add_metaclass(abc.ABCMeta)
class FragmentCompositionInterface(object):

    """
    Abstract class interface for a parent class that contains a
    Fragment.
    """

    def __init__(self, strict=False):
        self._fragment = Fragment(strict=strict)

    @property
    def fragment(self):
        return self._fragment

    @property
    def fragmentstr(self):
        """This method is deprecated. Use str(furl.fragment) instead."""
        s = ('furl.fragmentstr is deprecated. Use str(furl.fragment) instead. '
             'There should be one, and preferably only one, obvious way to '
             'serialize a Fragment object to a string.')
        warnings.warn(s, DeprecationWarning)
        return str(self._fragment)

    def __setattr__(self, attr, value):
        """
        Returns: True if this attribute is handled and set here, False
        otherwise.
        """
        if attr == 'fragment':
            self.fragment.load(value)
            return True
        return False


class furl(URLPathCompositionInterface, QueryCompositionInterface,
           FragmentCompositionInterface, UnicodeMixin):

    """
    Object for simple parsing and manipulation of a URL and its
    components.

      scheme://username:password@host:port/path?query#fragment

    Attributes:
      strict: Boolean whether or not UserWarnings should be raised if
        improperly encoded path, query, or fragment strings are provided
        to methods that take such strings, like load(), add(), set(),
        remove(), etc.
      username: Username string for authentication. Initially None.
      password: Password string for authentication with
        <username>. Initially None.
      scheme: URL scheme. A string ('http', 'https', '', etc) or None.
        All lowercase. Initially None.
      host: URL host (hostname, IPv4 address, or IPv6 address), not
        including port. All lowercase. Initially None.
      port: Port. Valid port values are 1-65535, or None meaning no port
        specified.
      netloc: Network location. Combined host and port string. Initially
      None.
      path: Path object from URLPathCompositionInterface.
      query: Query object from QueryCompositionInterface.
      fragment: Fragment object from FragmentCompositionInterface.
    """

    def __init__(self, url='', args=_absent, path=_absent, fragment=_absent,
                 scheme=_absent, netloc=_absent, origin=_absent,
                 fragment_path=_absent, fragment_args=_absent,
                 fragment_separator=_absent, host=_absent, port=_absent,
                 query=_absent, query_params=_absent, username=_absent,
                 password=_absent, strict=False):
        """
        Raises: ValueError on invalid URL or invalid URL component(s) provided.
        """
        URLPathCompositionInterface.__init__(self, strict=strict)
        QueryCompositionInterface.__init__(self, strict=strict)
        FragmentCompositionInterface.__init__(self, strict=strict)
        self.strict = strict

        self.load(url)  # Raises ValueError on invalid URL.
        self.set(  # Raises ValueError on invalid URL component(s).
            args=args, path=path, fragment=fragment, scheme=scheme,
            netloc=netloc, origin=origin, fragment_path=fragment_path,
            fragment_args=fragment_args, fragment_separator=fragment_separator,
            host=host, port=port, query=query, query_params=query_params,
            username=username, password=password)

    def load(self, url):
        """
        Parse and load a URL.

        Raises: ValueError on invalid URL, like a malformed IPv6 address
        or invalid port.
        """
        self.username = self.password = None
        self._host = self._port = self._scheme = None

        if url is None:
            url = ''
        if not isinstance(url, six.string_types):
            url = str(url)

        # urlsplit() raises a ValueError on malformed IPv6 addresses in
        # Python 2.7+.
        tokens = urlsplit(url)

        self.netloc = tokens.netloc  # Raises ValueError in Python 2.7+.
        self.scheme = tokens.scheme
        if not self.port:
            self._port = DEFAULT_PORTS.get(self.scheme)
        self.path.load(tokens.path)
        self.query.load(tokens.query)
        self.fragment.load(tokens.fragment)

        return self

    @property
    def scheme(self):
        return self._scheme

    @scheme.setter
    def scheme(self, scheme):
        if callable_attr(scheme, 'lower'):
            scheme = scheme.lower()
        self._scheme = scheme

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, host):
        """
        Raises: ValueError on invalid host or malformed IPv6 address.
        """
        # Invalid IPv6 literal.
        urllib.parse.urlsplit('http://%s/' % host)  # Raises ValueError.

        # Invalid host string.
        resembles_ipv6_literal = (
            host is not None and lget(host, 0) == '[' and ':' in host and
            lget(host, -1) == ']')
        if (host is not None and not resembles_ipv6_literal and
           not is_valid_host(host)):
            errmsg = (
                "Invalid host '%s'. Host strings must have at least one "
                "non-period character, can't contain any of '%s', and can't "
                "have adjacent periods.")
            raise ValueError(errmsg % (host, INVALID_HOST_CHARS))

        if callable_attr(host, 'lower'):
            host = host.lower()
        if callable_attr(host, 'startswith') and host.startswith('xn--'):
            host = idna_decode(host)
        self._host = host

    @property
    def port(self):
        return self._port or DEFAULT_PORTS.get(self.scheme)

    @port.setter
    def port(self, port):
        """
        The port value can be 1-65535 or None, meaning no port specified. If
        <port> is None and self.scheme is a known scheme in DEFAULT_PORTS,
        the default port value from DEFAULT_PORTS will be used.

        Raises: ValueError on invalid port.
        """
        if port is None:
            self._port = DEFAULT_PORTS.get(self.scheme)
        elif is_valid_port(port):
            self._port = int(str(port))
        else:
            raise ValueError("Invalid port '%s'." % port)

    @property
    def netloc(self):
        userpass = quote(utf8(self.username) or '', safe='')
        if self.password is not None:
            userpass += ':' + quote(utf8(self.password), safe='')
        if userpass or self.username is not None:
            userpass += '@'

        netloc = idna_encode(self.host)
        if self.port and self.port != DEFAULT_PORTS.get(self.scheme):
            netloc = (netloc or '') + (':' + str(self.port))

        if userpass or netloc:
            netloc = (userpass or '') + (netloc or '')

        return netloc

    @netloc.setter
    def netloc(self, netloc):
        """
        Params:
          netloc: Network location string, like 'google.com' or
            'user:pass@google.com:99'.
        Raises: ValueError on invalid port or malformed IPv6 address.
        """
        # Raises ValueError on malformed IPv6 addresses.
        urllib.parse.urlsplit('http://%s/' % netloc)

        username = password = host = port = None

        if netloc and '@' in netloc:
            userpass, netloc = netloc.split('@', 1)
            if ':' in userpass:
                username, password = userpass.split(':', 1)
            else:
                username = userpass

        if netloc and ':' in netloc:
            # IPv6 address literal.
            if ']' in netloc:
                colonpos, bracketpos = netloc.rfind(':'), netloc.rfind(']')
                if colonpos > bracketpos and colonpos != bracketpos + 1:
                    raise ValueError("Invalid netloc '%s'." % netloc)
                elif colonpos > bracketpos and colonpos == bracketpos + 1:
                    host, port = netloc.rsplit(':', 1)
                else:
                    host = netloc
            else:
                host, port = netloc.rsplit(':', 1)
                host = host
        else:
            host = netloc

        # Avoid side effects by assigning self.port before self.host so
        # that if an exception is raised when assigning self.port,
        # self.host isn't updated.
        self.port = port  # Raises ValueError on invalid port.
        self.host = host
        self.username = None if username is None else unquote(username)
        self.password = None if password is None else unquote(password)

    @property
    def origin(self):
        port = ''
        scheme = self.scheme or ''
        host = idna_encode(self.host) or ''
        if self.port and self.port != DEFAULT_PORTS.get(self.scheme):
            port = ':%s' % self.port
        origin = '%s://%s%s' % (scheme, host, port)

        return origin

    @origin.setter
    def origin(self, origin):
        if origin is None:
            self.scheme = self.netloc = None
        else:
            toks = origin.split('://', 1)
            if len(toks) == 1:
                host_port = origin
            else:
                self.scheme, host_port = toks

            if ':' in host_port:
                self.host, self.port = host_port.split(':', 1)
            else:
                self.host = host_port

    @property
    def url(self):
        return self.tostr()

    @url.setter
    def url(self, url):
        return self.load(url)

    def add(self, args=_absent, path=_absent, fragment_path=_absent,
            fragment_args=_absent, query_params=_absent):
        """
        Add components to a URL and return this furl instance, <self>.

        If both <args> and <query_params> are provided, a UserWarning is
        raised because <args> is provided as a shortcut for
        <query_params>, not to be used simultaneously with
        <query_params>. Nonetheless, providing both <args> and
        <query_params> behaves as expected, with query keys and values
        from both <args> and <query_params> added to the query - <args>
        first, then <query_params>.

        Parameters:
          args: Shortcut for <query_params>.
          path: A list of path segments to add to the existing path
            segments, or a path string to join with the existing path
            string.
          query_params: A dictionary of query keys and values or list of
            key:value items to add to the query.
          fragment_path: A list of path segments to add to the existing
            fragment path segments, or a path string to join with the
            existing fragment path string.
          fragment_args: A dictionary of query keys and values or list
            of key:value items to add to the fragment's query.

        Returns: <self>.

        Raises: UserWarning if redundant and possibly conflicting <args> and
        <query_params> were provided.
        """
        if args is not _absent and query_params is not _absent:
            s = ('Both <args> and <query_params> provided to furl.add(). '
                 '<args> is a shortcut for <query_params>, not to be used '
                 'with <query_params>. See furl.add() documentation for more '
                 'details.')
            warnings.warn(s, UserWarning)

        if path is not _absent:
            self.path.add(path)
        if args is not _absent:
            self.query.add(args)
        if query_params is not _absent:
            self.query.add(query_params)
        if fragment_path is not _absent or fragment_args is not _absent:
            self.fragment.add(path=fragment_path, args=fragment_args)

        return self

    def set(self, args=_absent, path=_absent, fragment=_absent, query=_absent,
            scheme=_absent, username=_absent, password=_absent, host=_absent,
            port=_absent, netloc=_absent, origin=_absent, query_params=_absent,
            fragment_path=_absent, fragment_args=_absent,
            fragment_separator=_absent):
        """
        Set components of a url and return this furl instance, <self>.

        If any overlapping, and hence possibly conflicting, parameters
        are provided, appropriate UserWarning's will be raised. The
        groups of parameters that could potentially overlap are

          <scheme> and <origin>
          <origin>, <netloc>, and/or (<host> or <port>)
          <fragment> and (<fragment_path> and/or <fragment_args>)
          any two or all of <query>, <args>, and/or <query_params>

        In all of the above groups, the latter parameter(s) take
        precedence over the earlier parameter(s). So, for example

          furl('http://google.com/').set(
            netloc='yahoo.com:99', host='bing.com', port=40)

        will result in a UserWarning being raised and the url becoming

          'http://bing.com:40/'

        not

          'http://yahoo.com:99/

        Parameters:
          args: Shortcut for <query_params>.
          path: A list of path segments or a path string to adopt.
          fragment: Fragment string to adopt.
          scheme: Scheme string to adopt.
          netloc: Network location string to adopt.
          origin: Scheme and netloc.
          query: Query string to adopt.
          query_params: A dictionary of query keys and values or list of
            key:value items to adopt.
          fragment_path: A list of path segments to adopt for the
            fragment's path or a path string to adopt as the fragment's
            path.
          fragment_args: A dictionary of query keys and values or list
            of key:value items for the fragment's query to adopt.
          fragment_separator: Boolean whether or not there should be a
            '?' separator between the fragment path and fragment query.
          host: Host string to adopt.
          port: Port number to adopt.
          username: Username string to adopt.
          password: Password string to adopt.
        Raises:
          ValueError on invalid port.
          UserWarning if <scheme> and <origin> are provided.
          UserWarning if <origin>, <netloc> and/or (<host> and/or <port>) are
            provided.
          UserWarning if <query>, <args>, and/or <query_params> are provided.
          UserWarning if <fragment> and (<fragment_path>,
            <fragment_args>, and/or <fragment_separator>) are provided.
        Returns: <self>.
        """
        def present(v):
            return v is not _absent

        if present(scheme) and present(origin):
            s = ('Possible parameter overlap: <scheme> and <origin>. See '
                 'furl.set() documentation for more details.')
            warnings.warn(s, UserWarning)
        provided = [
            present(netloc), present(origin), present(host) or present(port)]
        if sum(provided) >= 2:
            s = ('Possible parameter overlap: <origin>, <netloc> and/or '
                 '(<host> and/or <port>) provided. See furl.set() '
                 'documentation for more details.')
            warnings.warn(s, UserWarning)
        if sum(present(p) for p in [args, query, query_params]) >= 2:
            s = ('Possible parameter overlap: <query>, <args>, and/or '
                 '<query_params> provided. See furl.set() documentation for '
                 'more details.')
            warnings.warn(s, UserWarning)
        provided = [fragment_path, fragment_args, fragment_separator]
        if present(fragment) and any(present(p) for p in provided):
            s = ('Possible parameter overlap: <fragment> and '
                 '(<fragment_path>and/or <fragment_args>) or <fragment> '
                 'and <fragment_separator> provided. See furl.set() '
                 'documentation for more details.')
            warnings.warn(s, UserWarning)

        # Guard against side effects on exception.
        original_url = self.url
        try:
            if username is not _absent:
                self.username = username
            if password is not _absent:
                self.password = password
            if netloc is not _absent:
                # Raises ValueError on invalid port or malformed IP.
                self.netloc = netloc
            if origin is not _absent:
                # Raises ValueError on invalid port or malformed IP.
                self.origin = origin
            if scheme is not _absent:
                self.scheme = scheme
            if host is not _absent:
                # Raises ValueError on invalid host or malformed IP.
                self.host = host
            if port is not _absent:
                self.port = port  # Raises ValueError on invalid port.

            if path is not _absent:
                self.path.load(path)
            if query is not _absent:
                self.query.load(query)
            if args is not _absent:
                self.query.load(args)
            if query_params is not _absent:
                self.query.load(query_params)
            if fragment is not _absent:
                self.fragment.load(fragment)
            if fragment_path is not _absent:
                self.fragment.path.load(fragment_path)
            if fragment_args is not _absent:
                self.fragment.query.load(fragment_args)
            if fragment_separator is not _absent:
                self.fragment.separator = fragment_separator
        except Exception:
            self.load(original_url)
            raise

        return self

    def remove(self, args=_absent, path=_absent, fragment=_absent,
               query=_absent, scheme=False, username=False, password=False,
               host=False, port=False, netloc=False, origin=False,
               query_params=_absent, fragment_path=_absent,
               fragment_args=_absent):
        """
        Remove components of this furl's URL and return this furl
        instance, <self>.

        Parameters:
          args: Shortcut for query_params.
          path: A list of path segments to remove from the end of the
            existing path segments list, or a path string to remove from
            the end of the existing path string, or True to remove the
            path portion of the URL entirely.
          query: A list of query keys to remove from the query, if they
            exist, or True to remove the query portion of the URL
            entirely.
          query_params: A list of query keys to remove from the query,
            if they exist.
          port: If True, remove the port from the network location
            string, if it exists.
          fragment: If True, remove the fragment portion of the URL
            entirely.
          fragment_path: A list of path segments to remove from the end
            of the fragment's path segments or a path string to remove
            from the end of the fragment's path string.
          fragment_args: A list of query keys to remove from the
            fragment's query, if they exist.
          username: If True, remove the username, if it exists.
          password: If True, remove the password, if it exists.
        Returns: <self>.
        """
        if scheme is True:
            self.scheme = None
        if username is True:
            self.username = None
        if password is True:
            self.password = None
        if host is True:
            self.host = None
        if port is True:
            self.port = None
        if netloc is True:
            self.netloc = None
        if origin is True:
            self.origin = None

        if path is not _absent:
            self.path.remove(path)

        if args is not _absent:
            self.query.remove(args)
        if query is not _absent:
            self.query.remove(query)
        if query_params is not _absent:
            self.query.remove(query_params)

        if fragment is not _absent:
            self.fragment.remove(fragment)
        if fragment_path is not _absent:
            self.fragment.path.remove(fragment_path)
        if fragment_args is not _absent:
            self.fragment.query.remove(fragment_args)

        return self

    def tostr(self, query_delimiter='&', query_quote_plus=True,
              query_dont_quote=''):
        encoded_query = self.query.encode(
            query_delimiter, query_quote_plus, query_dont_quote)
        url = urllib.parse.urlunsplit((
            self.scheme or '',  # Must be text type in Python 3.
            self.netloc,
            str(self.path),
            encoded_query,
            str(self.fragment),
        ))

        # Differentiate between '' and None values for scheme and netloc.
        if self.scheme == '':
            url = ':' + url

        if self.netloc == '':
            if self.scheme is None:
                url = '//' + url
            elif strip_scheme(url) == '':
                url = url + '//'

        return str(url)

    def join(self, *urls):
        for url in urls:
            if not isinstance(url, six.string_types):
                url = str(url)
            newurl = urljoin(self.url, url)
            self.load(newurl)
        return self

    def copy(self):
        return self.__class__(self)

    def asdict(self):
        return {
            'url': self.url,
            'scheme': self.scheme,
            'username': self.username,
            'password': self.password,
            'host': self.host,
            'host_encoded': idna_encode(self.host),
            'port': self.port,
            'netloc': self.netloc,
            'origin': self.origin,
            'path': self.path.asdict(),
            'query': self.query.asdict(),
            'fragment': self.fragment.asdict(),
            }

    def __truediv__(self, path):
        return self.copy().add(path=path)

    def __eq__(self, other):
        try:
            return self.url == other.url
        except AttributeError:
            return None

    def __ne__(self, other):
        return not self == other

    def __setattr__(self, attr, value):
        if (not PathCompositionInterface.__setattr__(self, attr, value) and
           not QueryCompositionInterface.__setattr__(self, attr, value) and
           not FragmentCompositionInterface.__setattr__(self, attr, value)):
            object.__setattr__(self, attr, value)

    def __unicode__(self):
        return self.tostr()

    def __repr__(self):
        return "%s('%s')" % (self.__class__.__name__, str(self))
