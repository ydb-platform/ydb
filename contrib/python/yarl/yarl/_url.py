import re
import sys
import warnings
from collections.abc import Mapping, Sequence
from functools import _CacheInfo, lru_cache
from ipaddress import ip_address
from typing import TYPE_CHECKING, Any, TypedDict, TypeVar, Union, overload
from urllib.parse import SplitResult, parse_qsl, uses_relative

import idna
from multidict import MultiDict, MultiDictProxy
from propcache.api import under_cached_property as cached_property

from ._parse import USES_AUTHORITY, make_netloc, split_netloc, split_url, unsplit_result
from ._path import normalize_path, normalize_path_segments
from ._query import (
    Query,
    QueryVariable,
    SimpleQuery,
    get_str_query,
    get_str_query_from_iterable,
    get_str_query_from_sequence_iterable,
)
from ._quoters import (
    FRAGMENT_QUOTER,
    FRAGMENT_REQUOTER,
    PATH_QUOTER,
    PATH_REQUOTER,
    PATH_SAFE_UNQUOTER,
    PATH_UNQUOTER,
    QS_UNQUOTER,
    QUERY_QUOTER,
    QUERY_REQUOTER,
    QUOTER,
    REQUOTER,
    UNQUOTER,
    human_quote,
)

DEFAULT_PORTS = {"http": 80, "https": 443, "ws": 80, "wss": 443, "ftp": 21}
USES_RELATIVE = frozenset(uses_relative)

# Special schemes https://url.spec.whatwg.org/#special-scheme
# are not allowed to have an empty host https://url.spec.whatwg.org/#url-representation
SCHEME_REQUIRES_HOST = frozenset(("http", "https", "ws", "wss", "ftp"))


# reg-name: unreserved / pct-encoded / sub-delims
# this pattern matches anything that is *not* in those classes. and is only used
# on lower-cased ASCII values.
NOT_REG_NAME = re.compile(
    r"""
        # any character not in the unreserved or sub-delims sets, plus %
        # (validated with the additional check for pct-encoded sequences below)
        [^a-z0-9\-._~!$&'()*+,;=%]
    |
        # % only allowed if it is part of a pct-encoded
        # sequence of 2 hex digits.
        %(?![0-9a-f]{2})
    """,
    re.VERBOSE,
)

_T = TypeVar("_T")

if sys.version_info >= (3, 11):
    from typing import Self
else:
    Self = Any


class CacheInfo(TypedDict):
    """Host encoding cache."""

    idna_encode: _CacheInfo
    idna_decode: _CacheInfo
    ip_address: _CacheInfo
    host_validate: _CacheInfo
    encode_host: _CacheInfo


class _InternalURLCache(TypedDict, total=False):
    _origin: "URL"
    absolute: bool
    scheme: str
    raw_authority: str
    _default_port: Union[int, None]
    authority: str
    raw_user: Union[str, None]
    user: Union[str, None]
    raw_password: Union[str, None]
    password: Union[str, None]
    raw_host: Union[str, None]
    host: Union[str, None]
    host_subcomponent: Union[str, None]
    port: Union[int, None]
    explicit_port: Union[int, None]
    raw_path: str
    path: str
    _parsed_query: list[tuple[str, str]]
    query: "MultiDictProxy[str]"
    raw_query_string: str
    query_string: str
    path_qs: str
    raw_path_qs: str
    raw_fragment: str
    fragment: str
    raw_parts: tuple[str, ...]
    parts: tuple[str, ...]
    parent: "URL"
    raw_name: str
    name: str
    raw_suffix: str
    suffix: str
    raw_suffixes: tuple[str, ...]
    suffixes: tuple[str, ...]


def rewrite_module(obj: _T) -> _T:
    obj.__module__ = "yarl"
    return obj


@lru_cache
def encode_url(url_str: str) -> tuple[SplitResult, _InternalURLCache]:
    """Parse unencoded URL."""
    cache: _InternalURLCache = {}
    host: Union[str, None]
    scheme, netloc, path, query, fragment = split_url(url_str)
    if not netloc:  # netloc
        host = ""
    else:
        if ":" in netloc or "@" in netloc or "[" in netloc:
            # Complex netloc
            username, password, host, port = split_netloc(netloc)
        else:
            username = password = port = None
            host = netloc
        if host is None:
            if scheme in SCHEME_REQUIRES_HOST:
                msg = (
                    "Invalid URL: host is required for "
                    f"absolute urls with the {scheme} scheme"
                )
                raise ValueError(msg)
            else:
                host = ""
        host = _encode_host(host, validate_host=False)
        # Remove brackets as host encoder adds back brackets for IPv6 addresses
        cache["raw_host"] = host[1:-1] if "[" in host else host
        cache["explicit_port"] = port
        if password is None and username is None:
            # Fast path for URLs without user, password
            netloc = host if port is None else f"{host}:{port}"
            cache["raw_user"] = None
            cache["raw_password"] = None
        else:
            raw_user = REQUOTER(username) if username else username
            raw_password = REQUOTER(password) if password else password
            netloc = make_netloc(raw_user, raw_password, host, port)
            cache["raw_user"] = raw_user
            cache["raw_password"] = raw_password

    if path:
        path = PATH_REQUOTER(path)
        if netloc:
            if "." in path:
                path = normalize_path(path)

    query = QUERY_REQUOTER(query) if query else query
    fragment = FRAGMENT_REQUOTER(fragment) if fragment else fragment
    cache["scheme"] = scheme
    cache["raw_query_string"] = query
    cache["raw_fragment"] = fragment
    # Constructing the tuple directly to avoid the overhead of
    # the lambda and arg processing since NamedTuples are constructed
    # with a run time built lambda
    # https://github.com/python/cpython/blob/d83fcf8371f2f33c7797bc8f5423a8bca8c46e5c/Lib/collections/__init__.py#L441
    return tuple.__new__(SplitResult, (scheme, netloc, path, query, fragment)), cache


@lru_cache
def pre_encoded_url(url_str: str) -> tuple[SplitResult, _InternalURLCache]:
    """Parse pre-encoded URL."""
    return tuple.__new__(SplitResult, split_url(url_str)), {}


@rewrite_module
class URL:
    # Don't derive from str
    # follow pathlib.Path design
    # probably URL will not suffer from pathlib problems:
    # it's intended for libraries like aiohttp,
    # not to be passed into standard library functions like os.open etc.

    # URL grammar (RFC 3986)
    # pct-encoded = "%" HEXDIG HEXDIG
    # reserved    = gen-delims / sub-delims
    # gen-delims  = ":" / "/" / "?" / "#" / "[" / "]" / "@"
    # sub-delims  = "!" / "$" / "&" / "'" / "(" / ")"
    #             / "*" / "+" / "," / ";" / "="
    # unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
    # URI         = scheme ":" hier-part [ "?" query ] [ "#" fragment ]
    # hier-part   = "//" authority path-abempty
    #             / path-absolute
    #             / path-rootless
    #             / path-empty
    # scheme      = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
    # authority   = [ userinfo "@" ] host [ ":" port ]
    # userinfo    = *( unreserved / pct-encoded / sub-delims / ":" )
    # host        = IP-literal / IPv4address / reg-name
    # IP-literal = "[" ( IPv6address / IPvFuture  ) "]"
    # IPvFuture  = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" )
    # IPv6address =                            6( h16 ":" ) ls32
    #             /                       "::" 5( h16 ":" ) ls32
    #             / [               h16 ] "::" 4( h16 ":" ) ls32
    #             / [ *1( h16 ":" ) h16 ] "::" 3( h16 ":" ) ls32
    #             / [ *2( h16 ":" ) h16 ] "::" 2( h16 ":" ) ls32
    #             / [ *3( h16 ":" ) h16 ] "::"    h16 ":"   ls32
    #             / [ *4( h16 ":" ) h16 ] "::"              ls32
    #             / [ *5( h16 ":" ) h16 ] "::"              h16
    #             / [ *6( h16 ":" ) h16 ] "::"
    # ls32        = ( h16 ":" h16 ) / IPv4address
    #             ; least-significant 32 bits of address
    # h16         = 1*4HEXDIG
    #             ; 16 bits of address represented in hexadecimal
    # IPv4address = dec-octet "." dec-octet "." dec-octet "." dec-octet
    # dec-octet   = DIGIT                 ; 0-9
    #             / %x31-39 DIGIT         ; 10-99
    #             / "1" 2DIGIT            ; 100-199
    #             / "2" %x30-34 DIGIT     ; 200-249
    #             / "25" %x30-35          ; 250-255
    # reg-name    = *( unreserved / pct-encoded / sub-delims )
    # port        = *DIGIT
    # path          = path-abempty    ; begins with "/" or is empty
    #               / path-absolute   ; begins with "/" but not "//"
    #               / path-noscheme   ; begins with a non-colon segment
    #               / path-rootless   ; begins with a segment
    #               / path-empty      ; zero characters
    # path-abempty  = *( "/" segment )
    # path-absolute = "/" [ segment-nz *( "/" segment ) ]
    # path-noscheme = segment-nz-nc *( "/" segment )
    # path-rootless = segment-nz *( "/" segment )
    # path-empty    = 0<pchar>
    # segment       = *pchar
    # segment-nz    = 1*pchar
    # segment-nz-nc = 1*( unreserved / pct-encoded / sub-delims / "@" )
    #               ; non-zero-length segment without any colon ":"
    # pchar         = unreserved / pct-encoded / sub-delims / ":" / "@"
    # query       = *( pchar / "/" / "?" )
    # fragment    = *( pchar / "/" / "?" )
    # URI-reference = URI / relative-ref
    # relative-ref  = relative-part [ "?" query ] [ "#" fragment ]
    # relative-part = "//" authority path-abempty
    #               / path-absolute
    #               / path-noscheme
    #               / path-empty
    # absolute-URI  = scheme ":" hier-part [ "?" query ]
    __slots__ = ("_cache", "_val")

    _val: SplitResult

    def __new__(
        cls,
        val: Union[str, SplitResult, "URL"] = "",
        *,
        encoded: bool = False,
        strict: Union[bool, None] = None,
    ) -> Self:
        if strict is not None:  # pragma: no cover
            warnings.warn("strict parameter is ignored")
        if type(val) is str:
            pass
        elif type(val) is cls:
            return val
        elif type(val) is SplitResult:
            if not encoded:
                raise ValueError("Cannot apply decoding to SplitResult")
            self = object.__new__(cls)
            self._val = val
            self._cache = {}
            return self
        elif isinstance(val, str):
            val = str(val)
        else:
            raise TypeError("Constructor parameter should be str")
        if encoded:
            split_result, cache = pre_encoded_url(val)
        else:
            split_result, cache = encode_url(val)
        self = object.__new__(cls)
        self._val = split_result
        self._cache = cache
        return self

    @classmethod
    def build(
        cls,
        *,
        scheme: str = "",
        authority: str = "",
        user: Union[str, None] = None,
        password: Union[str, None] = None,
        host: str = "",
        port: Union[int, None] = None,
        path: str = "",
        query: Union[Query, None] = None,
        query_string: str = "",
        fragment: str = "",
        encoded: bool = False,
    ) -> "URL":
        """Creates and returns a new URL"""

        if authority and (user or password or host or port):
            raise ValueError(
                'Can\'t mix "authority" with "user", "password", "host" or "port".'
            )
        if port is not None and not isinstance(port, int):
            raise TypeError("The port is required to be int.")
        if port and not host:
            raise ValueError('Can\'t build URL with "port" but without "host".')
        if query and query_string:
            raise ValueError('Only one of "query" or "query_string" should be passed')
        if (
            scheme is None
            or authority is None
            or host is None
            or path is None
            or query_string is None
            or fragment is None
        ):
            raise TypeError(
                'NoneType is illegal for "scheme", "authority", "host", "path", '
                '"query_string", and "fragment" args, use empty string instead.'
            )

        if encoded:
            if authority:
                netloc = authority
            elif host:
                if port is not None:
                    port = None if port == DEFAULT_PORTS.get(scheme) else port
                if user is None and password is None:
                    netloc = host if port is None else f"{host}:{port}"
                else:
                    netloc = make_netloc(user, password, host, port)
            else:
                netloc = ""
        else:  # not encoded
            _host: Union[str, None] = None
            if authority:
                user, password, _host, port = split_netloc(authority)
                _host = _encode_host(_host, validate_host=False) if _host else ""
            elif host:
                _host = _encode_host(host, validate_host=True)
            else:
                netloc = ""

            if _host is not None:
                if port is not None:
                    port = None if port == DEFAULT_PORTS.get(scheme) else port
                if user is None and password is None:
                    netloc = _host if port is None else f"{_host}:{port}"
                else:
                    netloc = make_netloc(user, password, _host, port, True)

            path = PATH_QUOTER(path) if path else path
            if path and netloc:
                if "." in path:
                    path = normalize_path(path)
                if path[0] != "/":
                    msg = (
                        "Path in a URL with authority should "
                        "start with a slash ('/') if set"
                    )
                    raise ValueError(msg)

            query_string = QUERY_QUOTER(query_string) if query_string else query_string
            fragment = FRAGMENT_QUOTER(fragment) if fragment else fragment

        if query:
            query_string = get_str_query(query) or ""

        url = object.__new__(cls)
        # Constructing the tuple directly to avoid the overhead of the lambda and
        # arg processing since NamedTuples are constructed with a run time built
        # lambda
        # https://github.com/python/cpython/blob/d83fcf8371f2f33c7797bc8f5423a8bca8c46e5c/Lib/collections/__init__.py#L441
        url._val = tuple.__new__(
            SplitResult, (scheme, netloc, path, query_string, fragment)
        )
        url._cache = {}
        return url

    @classmethod
    def _from_tup(cls, val: tuple[str, str, str, str, str]) -> "URL":
        """Create a new URL from a tuple.

        The tuple should be in the form of a SplitResult.

        (scheme, netloc, path, query, fragment)
        """
        self = object.__new__(cls)
        self._val = tuple.__new__(SplitResult, val)
        self._cache = {}
        return self

    def __init_subclass__(cls):
        raise TypeError(f"Inheriting a class {cls!r} from URL is forbidden")

    def __str__(self) -> str:
        val = self._val
        scheme, netloc, path, query, fragment = val
        if not val.path and val.netloc and (val.query or val.fragment):
            path = "/"
        if (port := self.explicit_port) is not None and port == self._default_port:
            # port normalization - using None for default ports to remove from rendering
            # https://datatracker.ietf.org/doc/html/rfc3986.html#section-6.2.3
            host = self.host_subcomponent
            netloc = make_netloc(self.raw_user, self.raw_password, host, None)
        return unsplit_result(scheme, netloc, path, query, fragment)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{str(self)}')"

    def __bytes__(self) -> bytes:
        return str(self).encode("ascii")

    def __eq__(self, other: object) -> bool:
        if type(other) is not URL:
            return NotImplemented

        val1 = self._val
        if not val1.path and val1.netloc:
            scheme, netloc, _, query, fragment = val1
            val1 = tuple.__new__(SplitResult, (scheme, netloc, "/", query, fragment))

        val2 = other._val
        if not val2.path and val2.netloc:
            scheme, netloc, _, query, fragment = val2
            val2 = tuple.__new__(SplitResult, (scheme, netloc, "/", query, fragment))

        return val1 == val2

    def __hash__(self) -> int:
        if (ret := self._cache.get("hash")) is None:
            val = self._val
            scheme, netloc, path, query, fragment = val
            if not path and netloc:
                val = tuple.__new__(SplitResult, (scheme, netloc, "/", query, fragment))
            ret = self._cache["hash"] = hash(val)
        return ret

    def __le__(self, other: object) -> bool:
        if type(other) is not URL:
            return NotImplemented
        return self._val <= other._val

    def __lt__(self, other: object) -> bool:
        if type(other) is not URL:
            return NotImplemented
        return self._val < other._val

    def __ge__(self, other: object) -> bool:
        if type(other) is not URL:
            return NotImplemented
        return self._val >= other._val

    def __gt__(self, other: object) -> bool:
        if type(other) is not URL:
            return NotImplemented
        return self._val > other._val

    def __truediv__(self, name: str) -> "URL":
        if not isinstance(name, str):
            return NotImplemented
        return self._make_child((str(name),))

    def __mod__(self, query: Query) -> "URL":
        return self.update_query(query)

    def __bool__(self) -> bool:
        val = self._val
        return bool(val.netloc or val.path or val.query or val.fragment)

    def __getstate__(self) -> tuple[SplitResult]:
        return (self._val,)

    def __setstate__(self, state):
        if state[0] is None and isinstance(state[1], dict):
            # default style pickle
            self._val = state[1]["_val"]
        else:
            self._val, *unused = state
        self._cache = {}

    def _cache_netloc(self) -> None:
        """Cache the netloc parts of the URL."""
        c = self._cache
        split_loc = split_netloc(self._val.netloc)
        c["raw_user"], c["raw_password"], c["raw_host"], c["explicit_port"] = split_loc

    def is_absolute(self) -> bool:
        """A check for absolute URLs.

        Return True for absolute ones (having scheme or starting
        with //), False otherwise.

        Is is preferred to call the .absolute property instead
        as it is cached.
        """
        return self.absolute

    def is_default_port(self) -> bool:
        """A check for default port.

        Return True if port is default for specified scheme,
        e.g. 'http://python.org' or 'http://python.org:80', False
        otherwise.

        Return False for relative URLs.

        """
        if (explicit := self.explicit_port) is None:
            # If the explicit port is None, then the URL must be
            # using the default port unless its a relative URL
            # which does not have an implicit port / default port
            return self._val.netloc != ""
        return explicit == self._default_port

    def origin(self) -> "URL":
        """Return an URL with scheme, host and port parts only.

        user, password, path, query and fragment are removed.

        """
        # TODO: add a keyword-only option for keeping user/pass maybe?
        return self._origin

    @cached_property
    def _origin(self) -> "URL":
        """Return an URL with scheme, host and port parts only.

        user, password, path, query and fragment are removed.
        """
        scheme, netloc, path, query, fragment = self._val
        if not netloc:
            raise ValueError("URL should be absolute")
        if not scheme:
            raise ValueError("URL should have scheme")
        if "@" in netloc:
            encoded_host = self.host_subcomponent
            netloc = make_netloc(None, None, encoded_host, self.explicit_port)
        elif not path and not query and not fragment:
            return self
        return self._from_tup((scheme, netloc, "", "", ""))

    def relative(self) -> "URL":
        """Return a relative part of the URL.

        scheme, user, password, host and port are removed.

        """
        _, netloc, path, query, fragment = self._val
        if not netloc:
            raise ValueError("URL should be absolute")
        return self._from_tup(("", "", path, query, fragment))

    @cached_property
    def absolute(self) -> bool:
        """A check for absolute URLs.

        Return True for absolute ones (having scheme or starting
        with //), False otherwise.

        """
        # `netloc`` is an empty string for relative URLs
        # Checking `netloc` is faster than checking `hostname`
        # because `hostname` is a property that does some extra work
        # to parse the host from the `netloc`
        return self._val.netloc != ""

    @cached_property
    def scheme(self) -> str:
        """Scheme for absolute URLs.

        Empty string for relative URLs or URLs starting with //

        """
        return self._val.scheme

    @cached_property
    def raw_authority(self) -> str:
        """Encoded authority part of URL.

        Empty string for relative URLs.

        """
        return self._val.netloc

    @cached_property
    def _default_port(self) -> Union[int, None]:
        """Default port for the scheme or None if not known."""
        return DEFAULT_PORTS.get(self._val.scheme)

    @cached_property
    def authority(self) -> str:
        """Decoded authority part of URL.

        Empty string for relative URLs.

        """
        return make_netloc(self.user, self.password, self.host, self.port)

    @cached_property
    def raw_user(self) -> Union[str, None]:
        """Encoded user part of URL.

        None if user is missing.

        """
        # not .username
        self._cache_netloc()
        return self._cache["raw_user"]

    @cached_property
    def user(self) -> Union[str, None]:
        """Decoded user part of URL.

        None if user is missing.

        """
        if (raw_user := self.raw_user) is None:
            return None
        return UNQUOTER(raw_user)

    @cached_property
    def raw_password(self) -> Union[str, None]:
        """Encoded password part of URL.

        None if password is missing.

        """
        self._cache_netloc()
        return self._cache["raw_password"]

    @cached_property
    def password(self) -> Union[str, None]:
        """Decoded password part of URL.

        None if password is missing.

        """
        if (raw_password := self.raw_password) is None:
            return None
        return UNQUOTER(raw_password)

    @cached_property
    def raw_host(self) -> Union[str, None]:
        """Encoded host part of URL.

        None for relative URLs.

        When working with IPv6 addresses, use the `host_subcomponent` property instead
        as it will return the host subcomponent with brackets.
        """
        # Use host instead of hostname for sake of shortness
        # May add .hostname prop later
        self._cache_netloc()
        return self._cache["raw_host"]

    @cached_property
    def host(self) -> Union[str, None]:
        """Decoded host part of URL.

        None for relative URLs.

        """
        if (raw := self.raw_host) is None:
            return None
        if raw and raw[-1].isdigit() or ":" in raw:
            # IP addresses are never IDNA encoded
            return raw
        return _idna_decode(raw)

    @cached_property
    def host_subcomponent(self) -> Union[str, None]:
        """Return the host subcomponent part of URL.

        None for relative URLs.

        https://datatracker.ietf.org/doc/html/rfc3986#section-3.2.2

        `IP-literal = "[" ( IPv6address / IPvFuture  ) "]"`

        Examples:
        - `http://example.com:8080` -> `example.com`
        - `http://example.com:80` -> `example.com`
        - `https://127.0.0.1:8443` -> `127.0.0.1`
        - `https://[::1]:8443` -> `[::1]`
        - `http://[::1]` -> `[::1]`

        """
        if (raw := self.raw_host) is None:
            return None
        return f"[{raw}]" if ":" in raw else raw

    @cached_property
    def port(self) -> Union[int, None]:
        """Port part of URL, with scheme-based fallback.

        None for relative URLs or URLs without explicit port and
        scheme without default port substitution.

        """
        return self.explicit_port or self._default_port

    @cached_property
    def explicit_port(self) -> Union[int, None]:
        """Port part of URL, without scheme-based fallback.

        None for relative URLs or URLs without explicit port.

        """
        self._cache_netloc()
        return self._cache["explicit_port"]

    @cached_property
    def raw_path(self) -> str:
        """Encoded path of URL.

        / for absolute URLs without path part.

        """
        return "/" if not (path := self._val.path) and self._val.netloc else path

    @cached_property
    def path(self) -> str:
        """Decoded path of URL.

        / for absolute URLs without path part.

        """
        return PATH_UNQUOTER(self.raw_path)

    @cached_property
    def path_safe(self) -> str:
        """Decoded path of URL.

        / for absolute URLs without path part.

        / (%2F) and % (%25) are not decoded

        """
        return PATH_SAFE_UNQUOTER(self.raw_path)

    @cached_property
    def _parsed_query(self) -> list[tuple[str, str]]:
        """Parse query part of URL."""
        return parse_qsl(self._val.query, keep_blank_values=True)

    @cached_property
    def query(self) -> "MultiDictProxy[str]":
        """A MultiDictProxy representing parsed query parameters in decoded
        representation.

        Empty value if URL has no query part.

        """
        return MultiDictProxy(MultiDict(self._parsed_query))

    @cached_property
    def raw_query_string(self) -> str:
        """Encoded query part of URL.

        Empty string if query is missing.

        """
        return self._val.query

    @cached_property
    def query_string(self) -> str:
        """Decoded query part of URL.

        Empty string if query is missing.

        """
        return QS_UNQUOTER(self._val.query)

    @cached_property
    def path_qs(self) -> str:
        """Decoded path of URL with query."""
        return self.path if not (q := self.query_string) else f"{self.path}?{q}"

    @cached_property
    def raw_path_qs(self) -> str:
        """Encoded path of URL with query."""
        return self.raw_path if not (q := self._val.query) else f"{self.raw_path}?{q}"

    @cached_property
    def raw_fragment(self) -> str:
        """Encoded fragment part of URL.

        Empty string if fragment is missing.

        """
        return self._val.fragment

    @cached_property
    def fragment(self) -> str:
        """Decoded fragment part of URL.

        Empty string if fragment is missing.

        """
        return UNQUOTER(self._val.fragment)

    @cached_property
    def raw_parts(self) -> tuple[str, ...]:
        """A tuple containing encoded *path* parts.

        ('/',) for absolute URLs if *path* is missing.

        """
        path = self._val.path
        if self._val.netloc:
            return ("/", *path[1:].split("/")) if path else ("/",)
        if path and path[0] == "/":
            return ("/", *path[1:].split("/"))
        return tuple(path.split("/"))

    @cached_property
    def parts(self) -> tuple[str, ...]:
        """A tuple containing decoded *path* parts.

        ('/',) for absolute URLs if *path* is missing.

        """
        return tuple(UNQUOTER(part) for part in self.raw_parts)

    @cached_property
    def parent(self) -> "URL":
        """A new URL with last part of path removed and cleaned up query and
        fragment.

        """
        scheme, netloc, path, query, fragment = self._val
        if not path or path == "/":
            if fragment or query:
                return self._from_tup((scheme, netloc, path, "", ""))
            return self
        parts = path.split("/")
        return self._from_tup((scheme, netloc, "/".join(parts[:-1]), "", ""))

    @cached_property
    def raw_name(self) -> str:
        """The last part of raw_parts."""
        parts = self.raw_parts
        if not self._val.netloc:
            return parts[-1]
        parts = parts[1:]
        return parts[-1] if parts else ""

    @cached_property
    def name(self) -> str:
        """The last part of parts."""
        return UNQUOTER(self.raw_name)

    @cached_property
    def raw_suffix(self) -> str:
        name = self.raw_name
        i = name.rfind(".")
        return name[i:] if 0 < i < len(name) - 1 else ""

    @cached_property
    def suffix(self) -> str:
        return UNQUOTER(self.raw_suffix)

    @cached_property
    def raw_suffixes(self) -> tuple[str, ...]:
        name = self.raw_name
        if name.endswith("."):
            return ()
        name = name.lstrip(".")
        return tuple("." + suffix for suffix in name.split(".")[1:])

    @cached_property
    def suffixes(self) -> tuple[str, ...]:
        return tuple(UNQUOTER(suffix) for suffix in self.raw_suffixes)

    def _make_child(self, paths: "Sequence[str]", encoded: bool = False) -> "URL":
        """
        add paths to self._val.path, accounting for absolute vs relative paths,
        keep existing, but do not create new, empty segments
        """
        parsed: list[str] = []
        needs_normalize: bool = False
        for idx, path in enumerate(reversed(paths)):
            # empty segment of last is not removed
            last = idx == 0
            if path and path[0] == "/":
                raise ValueError(
                    f"Appending path {path!r} starting from slash is forbidden"
                )
            # We need to quote the path if it is not already encoded
            # This cannot be done at the end because the existing
            # path is already quoted and we do not want to double quote
            # the existing path.
            path = path if encoded else PATH_QUOTER(path)
            needs_normalize |= "." in path
            segments = path.split("/")
            segments.reverse()
            # remove trailing empty segment for all but the last path
            segment_slice_start = int(not last and segments[0] == "")
            parsed += segments[segment_slice_start:]
        parsed.reverse()

        scheme, netloc, path, _, _ = self._val
        if path and (old_path_segments := path.split("/")):
            # If the old path ends with a slash, the last segment is an empty string
            # and should be removed before adding the new path segments.
            old_path_cutoff = -1 if old_path_segments[-1] == "" else None
            parsed = [*old_path_segments[:old_path_cutoff], *parsed]

        if netloc := netloc:
            # If the netloc is present, we need to ensure that the path is normalized
            parsed = normalize_path_segments(parsed) if needs_normalize else parsed
            if parsed and parsed[0] != "":
                # inject a leading slash when adding a path to an absolute URL
                # where there was none before
                parsed = ["", *parsed]

        new_path = "/".join(parsed)

        return self._from_tup((scheme, netloc, new_path, "", ""))

    def with_scheme(self, scheme: str) -> "URL":
        """Return a new URL with scheme replaced."""
        # N.B. doesn't cleanup query/fragment
        if not isinstance(scheme, str):
            raise TypeError("Invalid scheme type")
        lower_scheme = scheme.lower()
        _, netloc, path, query, fragment = self._val
        if not netloc and lower_scheme in SCHEME_REQUIRES_HOST:
            msg = (
                "scheme replacement is not allowed for "
                f"relative URLs for the {lower_scheme} scheme"
            )
            raise ValueError(msg)
        return self._from_tup((lower_scheme, netloc, path, query, fragment))

    def with_user(self, user: Union[str, None]) -> "URL":
        """Return a new URL with user replaced.

        Autoencode user if needed.

        Clear user/password if user is None.

        """
        # N.B. doesn't cleanup query/fragment
        scheme, netloc, path, query, fragment = self._val
        if user is None:
            password = None
        elif isinstance(user, str):
            user = QUOTER(user)
            password = self.raw_password
        else:
            raise TypeError("Invalid user type")
        if not netloc:
            raise ValueError("user replacement is not allowed for relative URLs")
        encoded_host = self.host_subcomponent or ""
        netloc = make_netloc(user, password, encoded_host, self.explicit_port)
        return self._from_tup((scheme, netloc, path, query, fragment))

    def with_password(self, password: Union[str, None]) -> "URL":
        """Return a new URL with password replaced.

        Autoencode password if needed.

        Clear password if argument is None.

        """
        # N.B. doesn't cleanup query/fragment
        if password is None:
            pass
        elif isinstance(password, str):
            password = QUOTER(password)
        else:
            raise TypeError("Invalid password type")
        scheme, netloc, path, query, fragment = self._val
        if not netloc:
            raise ValueError("password replacement is not allowed for relative URLs")
        encoded_host = self.host_subcomponent or ""
        port = self.explicit_port
        netloc = make_netloc(self.raw_user, password, encoded_host, port)
        return self._from_tup((scheme, netloc, path, query, fragment))

    def with_host(self, host: str) -> "URL":
        """Return a new URL with host replaced.

        Autoencode host if needed.

        Changing host for relative URLs is not allowed, use .join()
        instead.

        """
        # N.B. doesn't cleanup query/fragment
        if not isinstance(host, str):
            raise TypeError("Invalid host type")
        scheme, netloc, path, query, fragment = self._val
        if not netloc:
            raise ValueError("host replacement is not allowed for relative URLs")
        if not host:
            raise ValueError("host removing is not allowed")
        encoded_host = _encode_host(host, validate_host=True) if host else ""
        port = self.explicit_port
        netloc = make_netloc(self.raw_user, self.raw_password, encoded_host, port)
        return self._from_tup((scheme, netloc, path, query, fragment))

    def with_port(self, port: Union[int, None]) -> "URL":
        """Return a new URL with port replaced.

        Clear port to default if None is passed.

        """
        # N.B. doesn't cleanup query/fragment
        if port is not None:
            if isinstance(port, bool) or not isinstance(port, int):
                raise TypeError(f"port should be int or None, got {type(port)}")
            if not (0 <= port <= 65535):
                raise ValueError(f"port must be between 0 and 65535, got {port}")
        scheme, netloc, path, query, fragment = self._val
        if not netloc:
            raise ValueError("port replacement is not allowed for relative URLs")
        encoded_host = self.host_subcomponent or ""
        netloc = make_netloc(self.raw_user, self.raw_password, encoded_host, port)
        return self._from_tup((scheme, netloc, path, query, fragment))

    def with_path(self, path: str, *, encoded: bool = False) -> "URL":
        """Return a new URL with path replaced."""
        scheme, netloc, _, _, _ = self._val
        if not encoded:
            path = PATH_QUOTER(path)
            if netloc:
                path = normalize_path(path) if "." in path else path
        if path and path[0] != "/":
            path = f"/{path}"
        return self._from_tup((scheme, netloc, path, "", ""))

    @overload
    def with_query(self, query: Query) -> "URL": ...

    @overload
    def with_query(self, **kwargs: QueryVariable) -> "URL": ...

    def with_query(self, *args: Any, **kwargs: Any) -> "URL":
        """Return a new URL with query part replaced.

        Accepts any Mapping (e.g. dict, multidict.MultiDict instances)
        or str, autoencode the argument if needed.

        A sequence of (key, value) pairs is supported as well.

        It also can take an arbitrary number of keyword arguments.

        Clear query if None is passed.

        """
        # N.B. doesn't cleanup query/fragment
        query = get_str_query(*args, **kwargs) or ""
        scheme, netloc, path, _, fragment = self._val
        return self._from_tup((scheme, netloc, path, query, fragment))

    @overload
    def extend_query(self, query: Query) -> "URL": ...

    @overload
    def extend_query(self, **kwargs: QueryVariable) -> "URL": ...

    def extend_query(self, *args: Any, **kwargs: Any) -> "URL":
        """Return a new URL with query part combined with the existing.

        This method will not remove existing query parameters.

        Example:
        >>> url = URL('http://example.com/?a=1&b=2')
        >>> url.extend_query(a=3, c=4)
        URL('http://example.com/?a=1&b=2&a=3&c=4')
        """
        if not (new_query := get_str_query(*args, **kwargs)):
            return self
        scheme, netloc, path, query, fragment = self._val
        if query:
            # both strings are already encoded so we can use a simple
            # string join
            query += new_query if query[-1] == "&" else f"&{new_query}"
        else:
            query = new_query
        return self._from_tup((scheme, netloc, path, query, fragment))

    @overload
    def update_query(self, query: Query) -> "URL": ...

    @overload
    def update_query(self, **kwargs: QueryVariable) -> "URL": ...

    def update_query(self, *args: Any, **kwargs: Any) -> "URL":
        """Return a new URL with query part updated.

        This method will overwrite existing query parameters.

        Example:
        >>> url = URL('http://example.com/?a=1&b=2')
        >>> url.update_query(a=3, c=4)
        URL('http://example.com/?a=3&b=2&c=4')
        """
        in_query: Union[str, Mapping[str, QueryVariable], None]
        if kwargs:
            if args:
                msg = "Either kwargs or single query parameter must be present"
                raise ValueError(msg)
            in_query = kwargs
        elif len(args) == 1:
            in_query = args[0]
        else:
            raise ValueError("Either kwargs or single query parameter must be present")

        scheme, netloc, path, query, fragment = self._val
        if in_query is None:
            query = ""
        elif not in_query:
            pass
        elif isinstance(in_query, Mapping):
            qm: MultiDict[QueryVariable] = MultiDict(self._parsed_query)
            qm.update(in_query)
            query = get_str_query_from_sequence_iterable(qm.items())
        elif isinstance(in_query, str):
            qstr: MultiDict[str] = MultiDict(self._parsed_query)
            qstr.update(parse_qsl(in_query, keep_blank_values=True))
            query = get_str_query_from_iterable(qstr.items())
        elif isinstance(in_query, (bytes, bytearray, memoryview)):
            msg = "Invalid query type: bytes, bytearray and memoryview are forbidden"
            raise TypeError(msg)
        elif isinstance(in_query, Sequence):
            # We don't expect sequence values if we're given a list of pairs
            # already; only mappings like builtin `dict` which can't have the
            # same key pointing to multiple values are allowed to use
            # `_query_seq_pairs`.
            qs: MultiDict[SimpleQuery] = MultiDict(self._parsed_query)
            qs.update(in_query)
            query = get_str_query_from_iterable(qs.items())
        else:
            raise TypeError(
                "Invalid query type: only str, mapping or "
                "sequence of (key, value) pairs is allowed"
            )
        return self._from_tup((scheme, netloc, path, query, fragment))

    def without_query_params(self, *query_params: str) -> "URL":
        """Remove some keys from query part and return new URL."""
        params_to_remove = set(query_params) & self.query.keys()
        if not params_to_remove:
            return self
        return self.with_query(
            tuple(
                (name, value)
                for name, value in self.query.items()
                if name not in params_to_remove
            )
        )

    def with_fragment(self, fragment: Union[str, None]) -> "URL":
        """Return a new URL with fragment replaced.

        Autoencode fragment if needed.

        Clear fragment to default if None is passed.

        """
        # N.B. doesn't cleanup query/fragment
        if fragment is None:
            raw_fragment = ""
        elif not isinstance(fragment, str):
            raise TypeError("Invalid fragment type")
        else:
            raw_fragment = FRAGMENT_QUOTER(fragment)
        if self._val.fragment == raw_fragment:
            return self
        scheme, netloc, path, query, _ = self._val
        return self._from_tup((scheme, netloc, path, query, raw_fragment))

    def with_name(self, name: str) -> "URL":
        """Return a new URL with name (last part of path) replaced.

        Query and fragment parts are cleaned up.

        Name is encoded if needed.

        """
        # N.B. DOES cleanup query/fragment
        if not isinstance(name, str):
            raise TypeError("Invalid name type")
        if "/" in name:
            raise ValueError("Slash in name is not allowed")
        name = PATH_QUOTER(name)
        if name in (".", ".."):
            raise ValueError(". and .. values are forbidden")
        parts = list(self.raw_parts)
        scheme, netloc, _, _, _ = self._val
        if netloc:
            if len(parts) == 1:
                parts.append(name)
            else:
                parts[-1] = name
            parts[0] = ""  # replace leading '/'
        else:
            parts[-1] = name
            if parts[0] == "/":
                parts[0] = ""  # replace leading '/'
        return self._from_tup((scheme, netloc, "/".join(parts), "", ""))

    def with_suffix(self, suffix: str) -> "URL":
        """Return a new URL with suffix (file extension of name) replaced.

        Query and fragment parts are cleaned up.

        suffix is encoded if needed.
        """
        if not isinstance(suffix, str):
            raise TypeError("Invalid suffix type")
        if suffix and not suffix[0] == "." or suffix == ".":
            raise ValueError(f"Invalid suffix {suffix!r}")
        name = self.raw_name
        if not name:
            raise ValueError(f"{self!r} has an empty name")
        old_suffix = self.raw_suffix
        name = name + suffix if not old_suffix else name[: -len(old_suffix)] + suffix
        return self.with_name(name)

    def join(self, url: "URL") -> "URL":
        """Join URLs

        Construct a full (“absolute”) URL by combining a “base URL”
        (self) with another URL (url).

        Informally, this uses components of the base URL, in
        particular the addressing scheme, the network location and
        (part of) the path, to provide missing components in the
        relative URL.

        """
        if type(url) is not URL:
            raise TypeError("url should be URL")
        orig_scheme, orig_netloc, orig_path, orig_query, orig_fragment = self._val
        join_scheme, join_netloc, join_path, join_query, join_fragment = url._val
        scheme = join_scheme or orig_scheme

        if scheme != orig_scheme or scheme not in USES_RELATIVE:
            return url

        # scheme is in uses_authority as uses_authority is a superset of uses_relative
        if join_netloc and scheme in USES_AUTHORITY:
            return self._from_tup(
                (scheme, join_netloc, join_path, join_query, join_fragment)
            )

        fragment = join_fragment if join_path or join_fragment else orig_fragment
        query = join_query if join_path or join_query else orig_query

        if not join_path:
            path = orig_path
        else:
            if join_path[0] == "/":
                path = join_path
            elif not orig_path:
                path = f"/{join_path}"
            elif orig_path[-1] == "/":
                path = f"{orig_path}{join_path}"
            else:
                # …
                # and relativizing ".."
                # parts[0] is / for absolute urls,
                # this join will add a double slash there
                path = "/".join([*self.parts[:-1], ""]) + join_path
                # which has to be removed
                if orig_path[0] == "/":
                    path = path[1:]
            path = normalize_path(path) if "." in path else path

        return self._from_tup((scheme, orig_netloc, path, query, fragment))

    def joinpath(self, *other: str, encoded: bool = False) -> "URL":
        """Return a new URL with the elements in other appended to the path."""
        return self._make_child(other, encoded=encoded)

    def human_repr(self) -> str:
        """Return decoded human readable string for URL representation."""
        user = human_quote(self.user, "#/:?@[]")
        password = human_quote(self.password, "#/:?@[]")
        if (host := self.host) and ":" in host:
            host = f"[{host}]"
        path = human_quote(self.path, "#?")
        if TYPE_CHECKING:
            assert path is not None
        query_string = "&".join(
            "{}={}".format(human_quote(k, "#&+;="), human_quote(v, "#&+;="))
            for k, v in self.query.items()
        )
        fragment = human_quote(self.fragment, "")
        if TYPE_CHECKING:
            assert fragment is not None
        netloc = make_netloc(user, password, host, self.explicit_port)
        scheme = self._val.scheme
        return unsplit_result(scheme, netloc, path, query_string, fragment)


_DEFAULT_IDNA_SIZE = 256
_DEFAULT_ENCODE_SIZE = 512


@lru_cache(_DEFAULT_IDNA_SIZE)
def _idna_decode(raw: str) -> str:
    try:
        return idna.decode(raw.encode("ascii"))
    except UnicodeError:  # e.g. '::1'
        return raw.encode("ascii").decode("idna")


@lru_cache(_DEFAULT_IDNA_SIZE)
def _idna_encode(host: str) -> str:
    try:
        return idna.encode(host, uts46=True).decode("ascii")
    except UnicodeError:
        return host.encode("idna").decode("ascii")


@lru_cache(_DEFAULT_ENCODE_SIZE)
def _encode_host(host: str, validate_host: bool) -> str:
    """Encode host part of URL."""
    # If the host ends with a digit or contains a colon, its likely
    # an IP address.
    if host and (host[-1].isdigit() or ":" in host):
        raw_ip, sep, zone = host.partition("%")
        # If it looks like an IP, we check with _ip_compressed_version
        # and fall-through if its not an IP address. This is a performance
        # optimization to avoid parsing IP addresses as much as possible
        # because it is orders of magnitude slower than almost any other
        # operation this library does.
        # Might be an IP address, check it
        #
        # IP Addresses can look like:
        # https://datatracker.ietf.org/doc/html/rfc3986#section-3.2.2
        # - 127.0.0.1 (last character is a digit)
        # - 2001:db8::ff00:42:8329 (contains a colon)
        # - 2001:db8::ff00:42:8329%eth0 (contains a colon)
        # - [2001:db8::ff00:42:8329] (contains a colon -- brackets should
        #                             have been removed before it gets here)
        # Rare IP Address formats are not supported per:
        # https://datatracker.ietf.org/doc/html/rfc3986#section-7.4
        #
        # IP parsing is slow, so its wrapped in an LRU
        try:
            ip = ip_address(raw_ip)
        except ValueError:
            pass
        else:
            # These checks should not happen in the
            # LRU to keep the cache size small
            host = ip.compressed
            if ip.version == 6:
                return f"[{host}%{zone}]" if sep else f"[{host}]"
            return f"{host}%{zone}" if sep else host

    # IDNA encoding is slow, skip it for ASCII-only strings
    if host.isascii():
        # Check for invalid characters explicitly; _idna_encode() does this
        # for non-ascii host names.
        if validate_host and (invalid := NOT_REG_NAME.search(host)):
            value, pos, extra = invalid.group(), invalid.start(), ""
            if value == "@" or (value == ":" and "@" in host[pos:]):
                # this looks like an authority string
                extra = (
                    ", if the value includes a username or password, "
                    "use 'authority' instead of 'host'"
                )
            raise ValueError(
                f"Host {host!r} cannot contain {value!r} (at position {pos}){extra}"
            ) from None
        return host.lower()

    return _idna_encode(host)


@rewrite_module
def cache_clear() -> None:
    """Clear all LRU caches."""
    _idna_encode.cache_clear()
    _idna_decode.cache_clear()
    _encode_host.cache_clear()


@rewrite_module
def cache_info() -> CacheInfo:
    """Report cache statistics."""
    return {
        "idna_encode": _idna_encode.cache_info(),
        "idna_decode": _idna_decode.cache_info(),
        "ip_address": _encode_host.cache_info(),
        "host_validate": _encode_host.cache_info(),
        "encode_host": _encode_host.cache_info(),
    }


_SENTINEL = object()


@rewrite_module
def cache_configure(
    *,
    idna_encode_size: Union[int, None] = _DEFAULT_IDNA_SIZE,
    idna_decode_size: Union[int, None] = _DEFAULT_IDNA_SIZE,
    ip_address_size: Union[int, None, object] = _SENTINEL,
    host_validate_size: Union[int, None, object] = _SENTINEL,
    encode_host_size: Union[int, None, object] = _SENTINEL,
) -> None:
    """Configure LRU cache sizes."""
    global _idna_decode, _idna_encode, _encode_host
    # ip_address_size, host_validate_size are no longer
    # used, but are kept for backwards compatibility.
    if ip_address_size is not _SENTINEL or host_validate_size is not _SENTINEL:
        warnings.warn(
            "cache_configure() no longer accepts the "
            "ip_address_size or host_validate_size arguments, "
            "they are used to set the encode_host_size instead "
            "and will be removed in the future",
            DeprecationWarning,
            stacklevel=2,
        )

    if encode_host_size is not None:
        for size in (ip_address_size, host_validate_size):
            if size is None:
                encode_host_size = None
            elif encode_host_size is _SENTINEL:
                if size is not _SENTINEL:
                    encode_host_size = size
            elif size is not _SENTINEL:
                if TYPE_CHECKING:
                    assert isinstance(size, int)
                    assert isinstance(encode_host_size, int)
                encode_host_size = max(size, encode_host_size)
        if encode_host_size is _SENTINEL:
            encode_host_size = _DEFAULT_ENCODE_SIZE

    if TYPE_CHECKING:
        assert not isinstance(encode_host_size, object)
    _encode_host = lru_cache(encode_host_size)(_encode_host.__wrapped__)
    _idna_decode = lru_cache(idna_decode_size)(_idna_decode.__wrapped__)
    _idna_encode = lru_cache(idna_encode_size)(_idna_encode.__wrapped__)
