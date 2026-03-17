"""RFC 3986 compliant, scheme-agnostic replacement for `urllib.parse`.

This module defines RFC 3986 compliant replacements for the most
commonly used functions of the Python Standard Library
:mod:`urllib.parse` module.

"""

import collections
import collections.abc
import ipaddress
import numbers
import re
from string import hexdigits


__all__ = (
    "GEN_DELIMS",
    "RESERVED",
    "SUB_DELIMS",
    "UNRESERVED",
    "isabspath",
    "isabsuri",
    "isnetpath",
    "isrelpath",
    "issamedoc",
    "isuri",
    "uricompose",
    "uridecode",
    "uridefrag",
    "uriencode",
    "urijoin",
    "urisplit",
    "uriunsplit",
)

__version__ = "6.0.1"


# RFC 3986 2.2.  Reserved Characters
#
#   reserved    = gen-delims / sub-delims
#
#   gen-delims  = ":" / "/" / "?" / "#" / "[" / "]" / "@"
#
#   sub-delims  = "!" / "$" / "&" / "'" / "(" / ")"
#               / "*" / "+" / "," / ";" / "="
#
GEN_DELIMS = ":/?#[]@"
SUB_DELIMS = "!$&'()*+,;="
RESERVED = GEN_DELIMS + SUB_DELIMS

# RFC 3986 2.3.  Unreserved Characters
#
#   unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
#
UNRESERVED = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"

_unreserved = frozenset(UNRESERVED.encode())

# RFC 3986 2.1: For consistency, URI producers and normalizers should
# use uppercase hexadecimal digits for all percent-encodings.
_encoded = {
    b"": [
        bytes([i]) if i in _unreserved else ("%%%02X" % i).encode() for i in range(256)
    ]
}

_decoded = {
    (a + b).encode(): bytes.fromhex(a + b) for a in hexdigits for b in hexdigits
}


def uriencode(uristring, safe="", encoding="utf-8", errors="strict"):
    """Encode a URI string or string component."""
    if not isinstance(uristring, bytes):
        uristring = uristring.encode(encoding, errors)
    if not isinstance(safe, bytes):
        safe = safe.encode("ascii")
    try:
        encoded = _encoded[safe]
    except KeyError:
        encoded = _encoded[b""][:]
        for i in safe:
            encoded[i] = bytes([i])
        _encoded[safe] = encoded
    return b"".join(map(encoded.__getitem__, uristring))


def uridecode(uristring, encoding="utf-8", errors="strict"):
    """Decode a URI string or string component."""
    if not isinstance(uristring, bytes):
        uristring = uristring.encode(encoding or "ascii", errors)
    parts = uristring.split(b"%")
    result = [parts[0]]
    append = result.append
    decode = _decoded.get
    for s in parts[1:]:
        append(decode(s[:2], b"%" + s[:2]))
        append(s[2:])
    if encoding is not None:
        return b"".join(result).decode(encoding, errors)
    else:
        return b"".join(result)


class DefragResult(collections.namedtuple("DefragResult", "uri fragment")):
    """Class to hold :func:`uridefrag` results."""

    __slots__ = ()  # prevent creation of instance dictionary

    def geturi(self):
        """Return the recombined version of the original URI as a string."""
        fragment = self.fragment
        if fragment is None:
            return self.uri
        elif isinstance(fragment, bytes):
            return self.uri + b"#" + fragment
        else:
            return self.uri + "#" + fragment

    def getfragment(self, default=None, encoding="utf-8", errors="strict"):
        """Return the decoded fragment identifier, or `default` if the
        original URI did not contain a fragment component.

        """
        fragment = self.fragment
        if fragment is not None:
            return uridecode(fragment, encoding, errors)
        else:
            return default


class SplitResult(
    collections.namedtuple("SplitResult", "scheme authority path query fragment")
):
    """Base class to hold :func:`urisplit` results."""

    __slots__ = ()  # prevent creation of instance dictionary

    @property
    def userinfo(self):
        authority = self.authority
        if authority is None:
            return None
        userinfo, present, _ = authority.rpartition(self.AT)
        if present:
            return userinfo
        else:
            return None

    @property
    def host(self):
        authority = self.authority
        if authority is None:
            return None
        _, _, hostinfo = authority.rpartition(self.AT)
        host, _, port = hostinfo.rpartition(self.COLON)
        if port.lstrip(self.DIGITS):
            return hostinfo
        else:
            return host

    @property
    def port(self):
        authority = self.authority
        if authority is None:
            return None
        _, present, port = authority.rpartition(self.COLON)
        if present and not port.lstrip(self.DIGITS):
            return port
        else:
            return None

    def geturi(self):
        """Return the re-combined version of the original URI reference as a
        string.

        """
        scheme, authority, path, query, fragment = self

        # RFC 3986 5.3. Component Recomposition
        result = []
        if scheme is not None:
            result.extend([scheme, self.COLON])
        if authority is not None:
            result.extend([self.SLASH, self.SLASH, authority])
        result.append(path)
        if query is not None:
            result.extend([self.QUEST, query])
        if fragment is not None:
            result.extend([self.HASH, fragment])
        return self.EMPTY.join(result)

    def getscheme(self, default=None):
        """Return the URI scheme in canonical (lowercase) form, or `default`
        if the original URI reference did not contain a scheme component.

        """
        scheme = self.scheme
        if scheme is None:
            return default
        elif isinstance(scheme, bytes):
            return scheme.decode("ascii").lower()
        else:
            return scheme.lower()

    def getauthority(self, default=None, encoding="utf-8", errors="strict"):
        """Return the decoded userinfo, host and port subcomponents of the URI
        authority as a three-item tuple.

        """
        # TBD: (userinfo, host, port) kwargs, default string?
        if default is None:
            default = (None, None, None)
        elif not isinstance(default, collections.abc.Iterable):
            raise TypeError("Invalid default type")
        elif len(default) != 3:
            raise ValueError("Invalid default length")
        # TODO: this could be much more efficient by using a dedicated regex
        return (
            self.getuserinfo(default[0], encoding, errors),
            self.gethost(default[1], errors),
            self.getport(default[2]),
        )

    def getuserinfo(self, default=None, encoding="utf-8", errors="strict"):
        """Return the decoded userinfo subcomponent of the URI authority, or
        `default` if the original URI reference did not contain a
        userinfo field.

        """
        userinfo = self.userinfo
        if userinfo is None:
            return default
        else:
            return uridecode(userinfo, encoding, errors)

    def gethost(self, default=None, errors="strict"):
        """Return the decoded host subcomponent of the URI authority as a
        string or an :mod:`ipaddress` address object, or `default` if
        the original URI reference did not contain a host.

        """
        host = self.host
        if host is None or (not host and default is not None):
            return default
        elif host.startswith(self.LBRACKET) and host.endswith(self.RBRACKET):
            return self.__parse_ip_literal(host[1:-1])
        elif host.startswith(self.LBRACKET) or host.endswith(self.RBRACKET):
            raise ValueError("Invalid host %r: mismatched brackets" % host)
        # TODO: faster check for IPv4 address?
        try:
            if isinstance(host, bytes):
                return ipaddress.IPv4Address(host.decode("ascii"))
            else:
                return ipaddress.IPv4Address(host)
        except ValueError:
            return uridecode(host, "utf-8", errors).lower()

    def getport(self, default=None):
        """Return the port subcomponent of the URI authority as an
        :class:`int`, or `default` if the original URI reference did
        not contain a port or if the port was empty.

        """
        port = self.port
        if port:
            return int(port)
        else:
            return default

    def getpath(self, encoding="utf-8", errors="strict"):
        """Return the normalized decoded URI path."""
        path = self.__remove_dot_segments(self.path)
        return uridecode(path, encoding, errors)

    def getquery(self, default=None, encoding="utf-8", errors="strict"):
        """Return the decoded query string, or `default` if the original URI
        reference did not contain a query component.

        """
        query = self.query
        if query is None:
            return default
        else:
            return uridecode(query, encoding, errors)

    def getquerydict(self, sep="&", encoding="utf-8", errors="strict"):
        """Split the query component into individual `name=value` pairs
        separated by `sep` and return a dictionary of query variables.
        The dictionary keys are the unique query variable names and
        the values are lists of values for each name.

        """
        dict = collections.defaultdict(list)
        for name, value in self.getquerylist(sep, encoding, errors):
            dict[name].append(value)
        return dict

    def getquerylist(self, sep="&", encoding="utf-8", errors="strict"):
        """Split the query component into individual `name=value` pairs
        separated by `sep`, and return a list of `(name, value)`
        tuples.

        """
        if not self.query:
            return []
        elif isinstance(sep, type(self.query)):
            qsl = self.query.split(sep)
        elif isinstance(sep, bytes):
            qsl = self.query.split(sep.decode("ascii"))
        else:
            qsl = self.query.split(sep.encode("ascii"))
        items = []
        for parts in [qs.partition(self.EQ) for qs in qsl if qs]:
            name = uridecode(parts[0], encoding, errors)
            if parts[1]:
                value = uridecode(parts[2], encoding, errors)
            else:
                value = None
            items.append((name, value))
        return items

    def getfragment(self, default=None, encoding="utf-8", errors="strict"):
        """Return the decoded fragment identifier, or `default` if the
        original URI reference did not contain a fragment component.

        """
        fragment = self.fragment
        if fragment is None:
            return default
        else:
            return uridecode(fragment, encoding, errors)

    def isuri(self):
        """Return :const:`True` if this is a URI."""
        return self.scheme is not None

    def isabsuri(self):
        """Return :const:`True` if this is an absolute URI."""
        return self.scheme is not None and self.fragment is None

    def isnetpath(self):
        """Return :const:`True` if this is a network-path reference."""
        return self.scheme is None and self.authority is not None

    def isabspath(self):
        """Return :const:`True` if this is an absolute-path reference."""
        return (
            self.scheme is None
            and self.authority is None
            and self.path.startswith(self.SLASH)
        )

    def isrelpath(self):
        """Return :const:`True` if this is a relative-path reference."""
        return (
            self.scheme is None
            and self.authority is None
            and not self.path.startswith(self.SLASH)
        )

    def issamedoc(self):
        """Return :const:`True` if this is a same-document reference."""
        return (
            self.scheme is None
            and self.authority is None
            and not self.path
            and self.query is None
        )

    def transform(self, ref, strict=False):
        """Transform a URI reference relative to `self` into a
        :class:`SplitResult` representing its target URI.

        """
        scheme, authority, path, query, fragment = self.RE.match(ref).groups()

        # RFC 3986 5.2.2. Transform References
        if scheme is not None and (strict or scheme != self.scheme):
            path = self.__remove_dot_segments(path)
        elif authority is not None:
            scheme = self.scheme
            path = self.__remove_dot_segments(path)
        elif not path:
            scheme = self.scheme
            authority = self.authority
            path = self.path
            query = self.query if query is None else query
        elif path.startswith(self.SLASH):
            scheme = self.scheme
            authority = self.authority
            path = self.__remove_dot_segments(path)
        else:
            scheme = self.scheme
            authority = self.authority
            path = self.__remove_dot_segments(self.__merge(path))
        return type(self)(scheme, authority, path, query, fragment)

    def __merge(self, path):
        # RFC 3986 5.2.3. Merge Paths
        if self.authority is not None and not self.path:
            return self.SLASH + path
        else:
            parts = self.path.rpartition(self.SLASH)
            return parts[1].join((parts[0], path))

    @classmethod
    def __remove_dot_segments(cls, path):
        # RFC 3986 5.2.4. Remove Dot Segments
        pseg = []
        for s in path.split(cls.SLASH):
            if s == cls.DOT:
                continue
            elif s != cls.DOTDOT:
                pseg.append(s)
            elif len(pseg) == 1 and not pseg[0]:
                continue
            elif pseg and pseg[-1] != cls.DOTDOT:
                pseg.pop()
            else:
                pseg.append(s)
        # adjust for trailing '/.' or '/..'
        if path.rpartition(cls.SLASH)[2] in (cls.DOT, cls.DOTDOT):
            pseg.append(cls.EMPTY)
        if path and len(pseg) == 1 and pseg[0] == cls.EMPTY:
            pseg.insert(0, cls.DOT)
        return cls.SLASH.join(pseg)

    @classmethod
    def __parse_ip_literal(cls, address):
        # RFC 3986 3.2.2: In anticipation of future, as-yet-undefined
        # IP literal address formats, an implementation may use an
        # optional version flag to indicate such a format explicitly
        # rather than rely on heuristic determination.
        #
        #  IP-literal = "[" ( IPv6address / IPvFuture  ) "]"
        #
        #  IPvFuture  = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
        #
        # If a URI containing an IP-literal that starts with "v"
        # (case-insensitive), indicating that the version flag is
        # present, is dereferenced by an application that does not
        # know the meaning of that version flag, then the application
        # should return an appropriate error for "address mechanism
        # not supported".
        if isinstance(address, bytes):
            address = address.decode("ascii")
        if address.startswith("v"):
            raise ValueError("address mechanism not supported")
        return ipaddress.IPv6Address(address)


class SplitResultBytes(SplitResult):
    __slots__ = ()  # prevent creation of instance dictionary

    # RFC 3986 Appendix B
    RE = re.compile(
        rb"""
    (?:([A-Za-z][A-Za-z0-9+.-]*):)?  # scheme (RFC 3986 3.1)
    (?://([^/?#]*))?                 # authority
    ([^?#]*)                         # path
    (?:\?([^#]*))?                   # query
    (?:\#(.*))?                      # fragment
    """,
        flags=re.VERBOSE,
    )

    # RFC 3986 2.2 gen-delims
    COLON, SLASH, QUEST, HASH, LBRACKET, RBRACKET, AT = (
        b":",
        b"/",
        b"?",
        b"#",
        b"[",
        b"]",
        b"@",
    )

    # RFC 3986 3.3 dot-segments
    DOT, DOTDOT = b".", b".."

    EMPTY, EQ = b"", b"="

    DIGITS = b"0123456789"


class SplitResultString(SplitResult):
    __slots__ = ()  # prevent creation of instance dictionary

    # RFC 3986 Appendix B
    RE = re.compile(
        r"""
    (?:([A-Za-z][A-Za-z0-9+.-]*):)?  # scheme (RFC 3986 3.1)
    (?://([^/?#]*))?                 # authority
    ([^?#]*)                         # path
    (?:\?([^#]*))?                   # query
    (?:\#(.*))?                      # fragment
    """,
        flags=re.VERBOSE,
    )

    # RFC 3986 2.2 gen-delims
    COLON, SLASH, QUEST, HASH, LBRACKET, RBRACKET, AT = (
        ":",
        "/",
        "?",
        "#",
        "[",
        "]",
        "@",
    )

    # RFC 3986 3.3 dot-segments
    DOT, DOTDOT = ".", ".."

    EMPTY, EQ = "", "="

    DIGITS = "0123456789"


def uridefrag(uristring):
    """Remove an existing fragment component from a URI reference string."""
    if isinstance(uristring, bytes):
        parts = uristring.partition(b"#")
    else:
        parts = uristring.partition("#")
    return DefragResult(parts[0], parts[2] if parts[1] else None)


def urisplit(uristring):
    """Split a well-formed URI reference string into a tuple with five
    components corresponding to a URI's general structure::

      <scheme>://<authority>/<path>?<query>#<fragment>

    """
    if isinstance(uristring, bytes):
        result = SplitResultBytes
    else:
        result = SplitResultString
    return result(*result.RE.match(uristring).groups())


def uriunsplit(parts):
    """Combine the elements of a five-item iterable into a URI reference's
    string representation.

    """
    scheme, authority, path, query, fragment = parts
    if isinstance(path, bytes):
        result = SplitResultBytes
    else:
        result = SplitResultString
    return result(scheme, authority, path, query, fragment).geturi()


def urijoin(base, ref, strict=False):
    """Convert a URI reference relative to a base URI to its target URI
    string.

    """
    if isinstance(base, type(ref)):
        return urisplit(base).transform(ref, strict).geturi()
    elif isinstance(base, bytes):
        return urisplit(base.decode()).transform(ref, strict).geturi()
    else:
        return urisplit(base).transform(ref.decode(), strict).geturi()


def isuri(uristring):
    """Return :const:`True` if `uristring` is a URI."""
    return urisplit(uristring).isuri()


def isabsuri(uristring):
    """Return :const:`True` if `uristring` is an absolute URI."""
    return urisplit(uristring).isabsuri()


def isnetpath(uristring):
    """Return :const:`True` if `uristring` is a network-path reference."""
    return urisplit(uristring).isnetpath()


def isabspath(uristring):
    """Return :const:`True` if `uristring` is an absolute-path reference."""
    return urisplit(uristring).isabspath()


def isrelpath(uristring):
    """Return :const:`True` if `uristring` is a relative-path reference."""
    return urisplit(uristring).isrelpath()


def issamedoc(uristring):
    """Return :const:`True` if `uristring` is a same-document reference."""
    return urisplit(uristring).issamedoc()


# TBD: move compose to its own submodule?

# RFC 3986 3.1: scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
_SCHEME_RE = re.compile(b"^[A-Za-z][A-Za-z0-9+.-]*$")

# RFC 3986 3.2: authority = [ userinfo "@" ] host [ ":" port ]
_AUTHORITY_RE_BYTES = re.compile(b"^(?:(.*)@)?(.*?)(?::([0-9]*))?$")
_AUTHORITY_RE_STR = re.compile("^(?:(.*)@)?(.*?)(?::([0-9]*))?$")

# safe component characters
_SAFE_USERINFO = SUB_DELIMS + ":"
_SAFE_HOST = SUB_DELIMS
_SAFE_PATH = SUB_DELIMS + ":@/"
_SAFE_QUERY = SUB_DELIMS + ":@/?"
_SAFE_FRAGMENT = SUB_DELIMS + ":@/?"


def _scheme(scheme):
    if _SCHEME_RE.match(scheme):
        return scheme.lower()
    else:
        raise ValueError("Invalid scheme component")


def _authority(userinfo, host, port, encoding):
    authority = []

    if userinfo is not None:
        authority.append(uriencode(userinfo, _SAFE_USERINFO, encoding))
        authority.append(b"@")

    if isinstance(host, ipaddress.IPv6Address):
        authority.append(b"[" + host.compressed.encode() + b"]")
    elif isinstance(host, ipaddress.IPv4Address):
        authority.append(host.compressed.encode())
    elif isinstance(host, bytes):
        authority.append(_host(host))
    elif host is not None:
        authority.append(_host(host.encode("utf-8")))

    if isinstance(port, numbers.Number):
        authority.append(_port(str(port).encode()))
    elif isinstance(port, bytes):
        authority.append(_port(port))
    elif port is not None:
        authority.append(_port(port.encode()))

    return b"".join(authority) if authority else None


def _ip_literal(address):
    if address.startswith("v"):
        raise ValueError("Address mechanism not supported")
    else:
        return b"[" + ipaddress.IPv6Address(address).compressed.encode() + b"]"


def _host(host):
    # RFC 3986 3.2.3: Although host is case-insensitive, producers and
    # normalizers should use lowercase for registered names and
    # hexadecimal addresses for the sake of uniformity, while only
    # using uppercase letters for percent-encodings.
    if host.startswith(b"[") and host.endswith(b"]"):
        return _ip_literal(host[1:-1].decode())
    # check for IPv6 addresses as returned by SplitResult.gethost()
    try:
        return _ip_literal(host.decode("utf-8"))
    except ValueError:
        return uriencode(host.lower(), _SAFE_HOST, "utf-8")


def _port(port):
    # RFC 3986 3.2.3: URI producers and normalizers should omit the
    # port component and its ":" delimiter if port is empty or if its
    # value would be the same as that of the scheme's default.
    if port.lstrip(b"0123456789"):
        raise ValueError("Invalid port subcomponent")
    elif port:
        return b":" + port
    else:
        return b""


def _querylist(items, sep, encoding):
    terms = []
    append = terms.append
    safe = _SAFE_QUERY.replace(sep, "")
    for key, value in items:
        name = uriencode(key, safe, encoding)
        if value is None:
            append(name)
        elif isinstance(value, (bytes, str)):
            append(name + b"=" + uriencode(value, safe, encoding))
        else:
            append(name + b"=" + uriencode(str(value), safe, encoding))
    return sep.encode("ascii").join(terms)


def _querydict(mapping, sep, encoding):
    items = []
    for key, value in mapping.items():
        if isinstance(value, (bytes, str)):
            items.append((key, value))
        elif isinstance(value, collections.abc.Iterable):
            items.extend([(key, v) for v in value])
        else:
            items.append((key, value))
    return _querylist(items, sep, encoding)


def uricompose(
    scheme=None,
    authority=None,
    path="",
    query=None,
    fragment=None,
    userinfo=None,
    host=None,
    port=None,
    querysep="&",
    encoding="utf-8",
):
    """Compose a URI reference string from its individual components."""

    # RFC 3986 3.1: Scheme names consist of a sequence of characters
    # beginning with a letter and followed by any combination of
    # letters, digits, plus ("+"), period ("."), or hyphen ("-").
    # Although schemes are case-insensitive, the canonical form is
    # lowercase and documents that specify schemes must do so with
    # lowercase letters.  An implementation should accept uppercase
    # letters as equivalent to lowercase in scheme names (e.g., allow
    # "HTTP" as well as "http") for the sake of robustness but should
    # only produce lowercase scheme names for consistency.
    if isinstance(scheme, bytes):
        scheme = _scheme(scheme)
    elif scheme is not None:
        scheme = _scheme(scheme.encode())

    # authority must be string type or three-item iterable
    if authority is None:
        authority = (None, None, None)
    elif isinstance(authority, bytes):
        authority = _AUTHORITY_RE_BYTES.match(authority).groups()
    elif isinstance(authority, str):
        authority = _AUTHORITY_RE_STR.match(authority).groups()
    elif not isinstance(authority, collections.abc.Iterable):
        raise TypeError("Invalid authority type")
    elif len(authority) != 3:
        raise ValueError("Invalid authority length")
    authority = _authority(
        userinfo if userinfo is not None else authority[0],
        host if host is not None else authority[1],
        port if port is not None else authority[2],
        encoding,
    )

    # RFC 3986 3.3: If a URI contains an authority component, then the
    # path component must either be empty or begin with a slash ("/")
    # character.  If a URI does not contain an authority component,
    # then the path cannot begin with two slash characters ("//").
    path = uriencode(path, _SAFE_PATH, encoding)
    if authority is not None and path and not path.startswith(b"/"):
        raise ValueError("Invalid path with authority component")
    if authority is None and path.startswith(b"//"):
        raise ValueError("Invalid path without authority component")

    # RFC 3986 4.2: A path segment that contains a colon character
    # (e.g., "this:that") cannot be used as the first segment of a
    # relative-path reference, as it would be mistaken for a scheme
    # name.  Such a segment must be preceded by a dot-segment (e.g.,
    # "./this:that") to make a relative-path reference.
    if scheme is None and authority is None and not path.startswith(b"/"):
        if b":" in path.partition(b"/")[0]:
            path = b"./" + path

    # RFC 3986 3.4: The characters slash ("/") and question mark ("?")
    # may represent data within the query component.  Beware that some
    # older, erroneous implementations may not handle such data
    # correctly when it is used as the base URI for relative
    # references (Section 5.1), apparently because they fail to
    # distinguish query data from path data when looking for
    # hierarchical separators.  However, as query components are often
    # used to carry identifying information in the form of "key=value"
    # pairs and one frequently used value is a reference to another
    # URI, it is sometimes better for usability to avoid percent-
    # encoding those characters.
    if isinstance(query, (bytes, str)):
        query = uriencode(query, _SAFE_QUERY, encoding)
    elif isinstance(query, collections.abc.Mapping):
        query = _querydict(query, querysep, encoding)
    elif isinstance(query, collections.abc.Iterable):
        query = _querylist(query, querysep, encoding)
    elif query is not None:
        raise TypeError("Invalid query type")

    # RFC 3986 3.5: The characters slash ("/") and question mark ("?")
    # are allowed to represent data within the fragment identifier.
    # Beware that some older, erroneous implementations may not handle
    # this data correctly when it is used as the base URI for relative
    # references.
    if fragment is not None:
        fragment = uriencode(fragment, _SAFE_FRAGMENT, encoding)

    # return URI reference as `str`
    return uriunsplit((scheme, authority, path, query, fragment)).decode()
