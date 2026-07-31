import math
import re
import sys
import warnings
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from functools import _CacheInfo, lru_cache
from ipaddress import ip_address
from typing import (
    TYPE_CHECKING,
    Any,
    SupportsInt,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    overload,
)
from urllib.parse import (
    SplitResult,
    parse_qsl,
    quote,
    urlsplit,
    uses_netloc,
    uses_relative,
)

import idna
from multidict import MultiDict, MultiDictProxy, istr
from propcache.api import under_cached_property as cached_property

from ._quoting import _Quoter, _Unquoter

DEFAULT_PORTS = {"http": 80, "https": 443, "ws": 80, "wss": 443, "ftp": 21}
USES_AUTHORITY = frozenset(uses_netloc)
USES_RELATIVE = frozenset(uses_relative)

# Special schemes https://url.spec.whatwg.org/#special-scheme
# are not allowed to have an empty host https://url.spec.whatwg.org/#url-representation
SCHEME_REQUIRES_HOST = frozenset(("http", "https", "ws", "wss", "ftp"))

sentinel = object()

# reg-name: unreserved / pct-encoded / sub-delims
# this pattern matches anything that is *not* in those classes. and is only used
# on lower-cased ASCII values.
_not_reg_name = re.compile(
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

SimpleQuery = Union[str, int, float]
QueryVariable = Union[SimpleQuery, "Sequence[SimpleQuery]"]
Query = Union[
    None, str, "Mapping[str, QueryVariable]", "Sequence[Tuple[str, QueryVariable]]"
]
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


def _normalize_path_segments(segments: "Sequence[str]") -> list[str]:
    """Drop '.' and '..' from a sequence of str segments"""

    resolved_path: list[str] = []

    for seg in segments:
        if seg == "..":
            # ignore any .. segments that would otherwise cause an
            # IndexError when popped from resolved_path if
            # resolving for rfc3986
            with suppress(IndexError):
                resolved_path.pop()
        elif seg != ".":
            resolved_path.append(seg)

    if segments and segments[-1] in (".", ".."):
        # do some post-processing here.
        # if the last segment was a relative dir,
        # then we need to append the trailing '/'
        resolved_path.append("")

    return resolved_path


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

    _QUOTER = _Quoter(requote=False)
    _REQUOTER = _Quoter()
    _PATH_QUOTER = _Quoter(safe="@:", protected="/+", requote=False)
    _PATH_REQUOTER = _Quoter(safe="@:", protected="/+")
    _QUERY_QUOTER = _Quoter(safe="?/:@", protected="=+&;", qs=True, requote=False)
    _QUERY_REQUOTER = _Quoter(safe="?/:@", protected="=+&;", qs=True)
    _QUERY_PART_QUOTER = _Quoter(safe="?/:@", qs=True, requote=False)
    _FRAGMENT_QUOTER = _Quoter(safe="?/:@", requote=False)
    _FRAGMENT_REQUOTER = _Quoter(safe="?/:@")

    _UNQUOTER = _Unquoter()
    _PATH_UNQUOTER = _Unquoter(unsafe="+")
    _PATH_SAFE_UNQUOTER = _Unquoter(ignore="/%", unsafe="+")
    _QS_UNQUOTER = _Unquoter(qs=True)

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
            val = urlsplit(val)
        elif type(val) is cls:
            return val
        elif type(val) is SplitResult:
            if not encoded:
                raise ValueError("Cannot apply decoding to SplitResult")
        elif isinstance(val, str):
            val = urlsplit(str(val))
        else:
            raise TypeError("Constructor parameter should be str")

        cache: _InternalURLCache = {}
        if not encoded:
            host: Union[str, None]
            scheme, netloc, path, query, fragment = val
            if not netloc:  # netloc
                host = ""
            else:
                if ":" in netloc or "@" in netloc or "[" in netloc:
                    # Complex netloc
                    username, password, host, port = cls._split_netloc(netloc)
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
                host = cls._encode_host(host, validate_host=False)
                # Remove brackets as host encoder adds back brackets for IPv6 addresses
                cache["raw_host"] = host[1:-1] if "[" in host else host
                cache["explicit_port"] = port
                if password is None and username is None:
                    # Fast path for URLs without user, password
                    netloc = host if port is None else f"{host}:{port}"
                    cache["raw_user"] = None
                    cache["raw_password"] = None
                else:
                    raw_user = cls._REQUOTER(username) if username else username
                    raw_password = cls._REQUOTER(password) if password else password
                    netloc = cls._make_netloc(raw_user, raw_password, host, port)
                    cache["raw_user"] = raw_user
                    cache["raw_password"] = raw_password

            if path:
                path = cls._PATH_REQUOTER(path)
                if netloc:
                    if "." in path:
                        path = cls._normalize_path(path)
                    if path[0] != "/":
                        cls._raise_for_authority_missing_abs_path()

            query = cls._QUERY_REQUOTER(query) if query else query
            fragment = cls._FRAGMENT_REQUOTER(fragment) if fragment else fragment
            cache["scheme"] = scheme
            cache["raw_query_string"] = query
            cache["raw_fragment"] = fragment
            # There is a good chance that the SplitResult is already normalized
            # so we can avoid the extra work of creating a new SplitResult
            # if the input SplitResult is already normalized
            if (
                val.netloc != netloc
                or val.path != path
                or val.query != query
                or val.fragment != fragment
            ):
                # Constructing the tuple directly to avoid the overhead of
                # the lambda and arg processing since NamedTuples are constructed
                # with a run time built lambda
                # https://github.com/python/cpython/blob/d83fcf8371f2f33c7797bc8f5423a8bca8c46e5c/Lib/collections/__init__.py#L441
                val = tuple.__new__(
                    SplitResult, (scheme, netloc, path, query, fragment)
                )

        self = object.__new__(cls)
        self._val = val
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
                    netloc = cls._make_netloc(user, password, host, port)
            else:
                netloc = ""
        else:  # not encoded
            _host: Union[str, None] = None
            if authority:
                user, password, _host, port = cls._split_netloc(authority)
                _host = cls._encode_host(_host, validate_host=False) if _host else ""
            elif host:
                _host = cls._encode_host(host, validate_host=True)
            else:
                netloc = ""

            if _host is not None:
                if port is not None:
                    port = None if port == DEFAULT_PORTS.get(scheme) else port
                if user is None and password is None:
                    netloc = _host if port is None else f"{_host}:{port}"
                else:
                    netloc = cls._make_netloc(user, password, _host, port, True)

            path = cls._PATH_QUOTER(path) if path else path
            if path and netloc:
                if "." in path:
                    path = cls._normalize_path(path)
                if path[0] != "/":
                    cls._raise_for_authority_missing_abs_path()

            query_string = (
                cls._QUERY_QUOTER(query_string) if query_string else query_string
            )
            fragment = cls._FRAGMENT_QUOTER(fragment) if fragment else fragment

        if query:
            query_string = cls._get_str_query(query) or ""

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
    def _from_val(cls, val: SplitResult) -> "URL":
        """Create a new URL from a SplitResult."""
        self = object.__new__(cls)
        self._val = val
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
            netloc = self._make_netloc(self.raw_user, self.raw_password, host, None)
        return self._unsplit_result(scheme, netloc, path, query, fragment)

    @staticmethod
    def _unsplit_result(
        scheme: str, netloc: str, url: str, query: str, fragment: str
    ) -> str:
        """Unsplit a URL without any normalization."""
        if netloc or (scheme and scheme in USES_AUTHORITY) or url[:2] == "//":
            if url and url[:1] != "/":
                url = f"//{netloc or ''}/{url}"
            else:
                url = f"//{netloc or ''}{url}"
        if scheme:
            url = f"{scheme}:{url}"
        if query:
            url = f"{url}?{query}"
        return f"{url}#{fragment}" if fragment else url

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
        cache = self._cache
        (
            cache["raw_user"],
            cache["raw_password"],
            cache["raw_host"],
            cache["explicit_port"],
        ) = self._split_netloc(self._val.netloc)

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
            netloc = self._make_netloc(None, None, encoded_host, self.explicit_port)
        elif not path and not query and not fragment:
            return self
        return self._from_val(tuple.__new__(SplitResult, (scheme, netloc, "", "", "")))

    def relative(self) -> "URL":
        """Return a relative part of the URL.

        scheme, user, password, host and port are removed.

        """
        _, netloc, path, query, fragment = self._val
        if not netloc:
            raise ValueError("URL should be absolute")
        val = tuple.__new__(SplitResult, ("", "", path, query, fragment))
        return self._from_val(val)

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
        return self._make_netloc(self.user, self.password, self.host, self.port)

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
        raw_user = self.raw_user
        if raw_user is None:
            return None
        return self._UNQUOTER(raw_user)

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
        raw_password = self.raw_password
        if raw_password is None:
            return None
        return self._UNQUOTER(raw_password)

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
        ret = self._val.path
        if not ret and self._val.netloc:
            ret = "/"
        return ret

    @cached_property
    def path(self) -> str:
        """Decoded path of URL.

        / for absolute URLs without path part.

        """
        return self._PATH_UNQUOTER(self.raw_path)

    @cached_property
    def path_safe(self) -> str:
        """Decoded path of URL.

        / for absolute URLs without path part.

        / (%2F) and % (%25) are not decoded

        """
        return self._PATH_SAFE_UNQUOTER(self.raw_path)

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
        return self._QS_UNQUOTER(self._val.query)

    @cached_property
    def path_qs(self) -> str:
        """Decoded path of URL with query."""
        if not self.query_string:
            return self.path
        return f"{self.path}?{self.query_string}"

    @cached_property
    def raw_path_qs(self) -> str:
        """Encoded path of URL with query."""
        if not self._val.query:
            return self.raw_path
        return f"{self.raw_path}?{self._val.query}"

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
        return self._UNQUOTER(self._val.fragment)

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
        return tuple(self._UNQUOTER(part) for part in self.raw_parts)

    @cached_property
    def parent(self) -> "URL":
        """A new URL with last part of path removed and cleaned up query and
        fragment.

        """
        scheme, netloc, path, query, fragment = self._val
        if not path or path == "/":
            if fragment or query:
                val = tuple.__new__(SplitResult, (scheme, netloc, path, "", ""))
                return self._from_val(val)
            return self
        parts = path.split("/")
        val = tuple.__new__(SplitResult, (scheme, netloc, "/".join(parts[:-1]), "", ""))
        return self._from_val(val)

    @cached_property
    def raw_name(self) -> str:
        """The last part of raw_parts."""
        parts = self.raw_parts
        if self._val.netloc:
            parts = parts[1:]
            if not parts:
                return ""
            else:
                return parts[-1]
        else:
            return parts[-1]

    @cached_property
    def name(self) -> str:
        """The last part of parts."""
        return self._UNQUOTER(self.raw_name)

    @cached_property
    def raw_suffix(self) -> str:
        name = self.raw_name
        i = name.rfind(".")
        if 0 < i < len(name) - 1:
            return name[i:]
        else:
            return ""

    @cached_property
    def suffix(self) -> str:
        return self._UNQUOTER(self.raw_suffix)

    @cached_property
    def raw_suffixes(self) -> tuple[str, ...]:
        name = self.raw_name
        if name.endswith("."):
            return ()
        name = name.lstrip(".")
        return tuple("." + suffix for suffix in name.split(".")[1:])

    @cached_property
    def suffixes(self) -> tuple[str, ...]:
        return tuple(self._UNQUOTER(suffix) for suffix in self.raw_suffixes)

    @staticmethod
    def _raise_for_authority_missing_abs_path() -> None:
        """Raise when he path in URL with authority starts lacks a leading slash."""
        msg = "Path in a URL with authority should start with a slash ('/') if set"
        raise ValueError(msg)

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
            path = path if encoded else self._PATH_QUOTER(path)
            needs_normalize |= "." in path
            segments = path.split("/")
            segments.reverse()
            # remove trailing empty segment for all but the last path
            segment_slice_start = int(not last and segments[0] == "")
            parsed += segments[segment_slice_start:]
        parsed.reverse()

        v = self._val
        if v.path and (old_path_segments := v.path.split("/")):
            # If the old path ends with a slash, the last segment is an empty string
            # and should be removed before adding the new path segments.
            old_path_cutoff = -1 if old_path_segments[-1] == "" else None
            parsed = [*old_path_segments[:old_path_cutoff], *parsed]

        if netloc := v.netloc:
            # If the netloc is present, we need to ensure that the path is normalized
            parsed = _normalize_path_segments(parsed) if needs_normalize else parsed
            if parsed and parsed[0] != "":
                # inject a leading slash when adding a path to an absolute URL
                # where there was none before
                parsed = ["", *parsed]

        new_path = "/".join(parsed)

        return self._from_val(
            tuple.__new__(SplitResult, (v.scheme, netloc, new_path, "", ""))
        )

    @classmethod
    def _normalize_path(cls, path: str) -> str:
        # Drop '.' and '..' from str path
        prefix = ""
        if path and path[0] == "/":
            # preserve the "/" root element of absolute paths, copying it to the
            # normalised output as per sections 5.2.4 and 6.2.2.3 of rfc3986.
            prefix = "/"
            path = path[1:]

        segments = path.split("/")
        return prefix + "/".join(_normalize_path_segments(segments))

    @classmethod
    @lru_cache  # match the same size as urlsplit
    def _parse_host(
        cls, host: str
    ) -> tuple[bool, str, Union[bool, None], str, str, str]:
        """Parse host into parts

        Returns a tuple of:
        - True if the host looks like an IP address, False otherwise.
        - Lowercased host
        - True if the host is ASCII-only, False otherwise.
        - Raw IP address
        - Separator between IP address and zone
        - Zone part of the IP address
        """
        lower_host = host.lower()
        is_ascii = host.isascii()

        # If the host ends with a digit or contains a colon, its likely
        # an IP address.
        if host and (host[-1].isdigit() or ":" in host):
            if "%" in host:
                return True, lower_host, is_ascii, *host.partition("%")
            return True, lower_host, is_ascii, host, "", ""

        return False, lower_host, is_ascii, "", "", ""

    @classmethod
    def _encode_host(cls, host: str, validate_host: bool) -> str:
        """Encode host part of URL."""
        looks_like_ip, lower_host, is_ascii, raw_ip, sep, zone = cls._parse_host(host)
        if looks_like_ip:
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
                host, version = _ip_compressed_version(raw_ip)
            except ValueError:
                pass
            else:
                # These checks should not happen in the
                # LRU to keep the cache size small
                if version == 6:
                    return f"[{host}%{zone}]" if sep else f"[{host}]"
                return f"{host}%{zone}" if sep else host

        # IDNA encoding is slow,
        # skip it for ASCII-only strings
        # Don't move the check into _idna_encode() helper
        # to reduce the cache size
        if is_ascii:
            # Check for invalid characters explicitly; _idna_encode() does this
            # for non-ascii host names.
            if validate_host:
                _host_validate(lower_host)
            return lower_host

        return _idna_encode(lower_host)

    @classmethod
    @lru_cache  # match the same size as urlsplit
    def _make_netloc(
        cls,
        user: Union[str, None],
        password: Union[str, None],
        host: Union[str, None],
        port: Union[int, None],
        encode: bool = False,
    ) -> str:
        """Make netloc from parts.

        The user and password are encoded if encode is True.

        The host must already be encoded with _encode_host.
        """
        if host is None:
            return ""
        ret = host
        if port is not None:
            ret = f"{ret}:{port}"
        if user is None and password is None:
            return ret
        if password is not None:
            if not user:
                user = ""
            elif encode:
                user = cls._QUOTER(user)
            if encode:
                password = cls._QUOTER(password)
            user = f"{user}:{password}"
        elif user and encode:
            user = cls._QUOTER(user)
        return f"{user}@{ret}" if user else ret

    @classmethod
    @lru_cache  # match the same size as urlsplit
    def _split_netloc(
        cls,
        netloc: str,
    ) -> tuple[Union[str, None], Union[str, None], Union[str, None], Union[int, None]]:
        """Split netloc into username, password, host and port."""
        if "@" not in netloc:
            username: Union[str, None] = None
            password: Union[str, None] = None
            hostinfo = netloc
        else:
            userinfo, _, hostinfo = netloc.rpartition("@")
            username, have_password, password = userinfo.partition(":")
            if not have_password:
                password = None

        if "[" in hostinfo:
            _, _, bracketed = hostinfo.partition("[")
            hostname, _, port_str = bracketed.partition("]")
            _, _, port_str = port_str.partition(":")
        else:
            hostname, _, port_str = hostinfo.partition(":")

        if not port_str:
            return username or None, password, hostname or None, None

        try:
            port = int(port_str)
        except ValueError:
            raise ValueError("Invalid URL: port can't be converted to integer")
        if not (0 <= port <= 65535):
            raise ValueError("Port out of range 0-65535")
        return username or None, password, hostname or None, port

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
        val = tuple.__new__(SplitResult, (lower_scheme, netloc, path, query, fragment))
        return self._from_val(val)

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
            user = self._QUOTER(user)
            password = self.raw_password
        else:
            raise TypeError("Invalid user type")
        if not netloc:
            raise ValueError("user replacement is not allowed for relative URLs")
        encoded_host = self.host_subcomponent or ""
        netloc = self._make_netloc(user, password, encoded_host, self.explicit_port)
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, query, fragment))
        )

    def with_password(self, password: Union[str, None]) -> "URL":
        """Return a new URL with password replaced.

        Autoencode password if needed.

        Clear password if argument is None.

        """
        # N.B. doesn't cleanup query/fragment
        if password is None:
            pass
        elif isinstance(password, str):
            password = self._QUOTER(password)
        else:
            raise TypeError("Invalid password type")
        scheme, netloc, path, query, fragment = self._val
        if not netloc:
            raise ValueError("password replacement is not allowed for relative URLs")
        encoded_host = self.host_subcomponent or ""
        port = self.explicit_port
        netloc = self._make_netloc(self.raw_user, password, encoded_host, port)
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, query, fragment))
        )

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
        encoded_host = self._encode_host(host, validate_host=True) if host else ""
        port = self.explicit_port
        netloc = self._make_netloc(self.raw_user, self.raw_password, encoded_host, port)
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, query, fragment))
        )

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
        netloc = self._make_netloc(self.raw_user, self.raw_password, encoded_host, port)
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, query, fragment))
        )

    def with_path(self, path: str, *, encoded: bool = False) -> "URL":
        """Return a new URL with path replaced."""
        scheme, netloc, _, _, _ = self._val
        if not encoded:
            path = self._PATH_QUOTER(path)
            if netloc:
                path = self._normalize_path(path) if "." in path else path
        if path and path[0] != "/":
            path = f"/{path}"
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, "", ""))
        )

    @classmethod
    def _get_str_query_from_sequence_iterable(
        cls,
        items: Iterable[tuple[Union[str, istr], QueryVariable]],
    ) -> str:
        """Return a query string from a sequence of (key, value) pairs.

        value is a single value or a sequence of values for the key

        The sequence of values must be a list or tuple.
        """
        quoter = cls._QUERY_PART_QUOTER
        pairs = [
            f"{quoter(k)}={quoter(v if type(v) is str else cls._query_var(v))}"
            for k, val in items
            for v in (
                val
                if type(val) is not str and isinstance(val, (list, tuple))
                else (val,)
            )
        ]
        return "&".join(pairs)

    @staticmethod
    def _query_var(v: QueryVariable) -> str:
        cls = type(v)
        if cls is int:  # Fast path for non-subclassed int
            return str(v)
        if issubclass(cls, str):
            if TYPE_CHECKING:
                assert isinstance(v, str)
            return v
        if cls is float or issubclass(cls, float):
            if TYPE_CHECKING:
                assert isinstance(v, float)
            if math.isinf(v):
                raise ValueError("float('inf') is not supported")
            if math.isnan(v):
                raise ValueError("float('nan') is not supported")
            return str(float(v))
        if cls is not bool and isinstance(cls, SupportsInt):
            return str(int(v))
        raise TypeError(
            "Invalid variable type: value "
            "should be str, int or float, got {!r} "
            "of type {}".format(v, cls)
        )

    @classmethod
    def _get_str_query_from_iterable(
        cls, items: Iterable[tuple[Union[str, istr], SimpleQuery]]
    ) -> str:
        """Return a query string from an iterable.

        The iterable must contain (key, value) pairs.

        The values are not allowed to be sequences, only single values are
        allowed. For sequences, use `_get_str_query_from_sequence_iterable`.
        """
        quoter = cls._QUERY_PART_QUOTER
        # A listcomp is used since listcomps are inlined on CPython 3.12+ and
        # they are a bit faster than a generator expression.
        pairs = [
            f"{quoter(k)}={quoter(v if type(v) is str else cls._query_var(v))}"
            for k, v in items
        ]
        return "&".join(pairs)

    @classmethod
    def _get_str_query(cls, *args: Any, **kwargs: Any) -> Union[str, None]:
        query: Union[str, Mapping[str, QueryVariable], None]
        if kwargs:
            if args:
                msg = "Either kwargs or single query parameter must be present"
                raise ValueError(msg)
            query = kwargs
        elif len(args) == 1:
            query = args[0]
        else:
            raise ValueError("Either kwargs or single query parameter must be present")

        if query is None:
            return None
        if not query:
            return ""
        if isinstance(query, Mapping):
            return cls._get_str_query_from_sequence_iterable(query.items())
        if isinstance(query, str):
            return cls._QUERY_QUOTER(query)
        if isinstance(query, (bytes, bytearray, memoryview)):
            msg = "Invalid query type: bytes, bytearray and memoryview are forbidden"
            raise TypeError(msg)
        if isinstance(query, Sequence):
            # We don't expect sequence values if we're given a list of pairs
            # already; only mappings like builtin `dict` which can't have the
            # same key pointing to multiple values are allowed to use
            # `_query_seq_pairs`.
            return cls._get_str_query_from_iterable(query)
        raise TypeError(
            "Invalid query type: only str, mapping or "
            "sequence of (key, value) pairs is allowed"
        )

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
        query = self._get_str_query(*args, **kwargs) or ""
        scheme, netloc, path, _, fragment = self._val
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, query, fragment))
        )

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
        if not (new_query := self._get_str_query(*args, **kwargs)):
            return self
        scheme, netloc, path, query, fragment = self._val
        if query:
            # both strings are already encoded so we can use a simple
            # string join
            query += new_query if query[-1] == "&" else f"&{new_query}"
        else:
            query = new_query
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, query, fragment))
        )

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
            query = self._get_str_query_from_sequence_iterable(qm.items())
        elif isinstance(in_query, str):
            qstr: MultiDict[str] = MultiDict(self._parsed_query)
            qstr.update(parse_qsl(in_query, keep_blank_values=True))
            query = self._get_str_query_from_iterable(qstr.items())
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
            query = self._get_str_query_from_iterable(qs.items())
        else:
            raise TypeError(
                "Invalid query type: only str, mapping or "
                "sequence of (key, value) pairs is allowed"
            )
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, query, fragment))
        )

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
            raw_fragment = self._FRAGMENT_QUOTER(fragment)
        if self._val.fragment == raw_fragment:
            return self
        scheme, netloc, path, query, _ = self._val
        return self._from_val(
            tuple.__new__(SplitResult, (scheme, netloc, path, query, raw_fragment))
        )

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
        name = self._PATH_QUOTER(name)
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
        val = tuple.__new__(SplitResult, (scheme, netloc, "/".join(parts), "", ""))
        return self._from_val(val)

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
            return self._from_val(
                tuple.__new__(
                    SplitResult,
                    (scheme, join_netloc, join_path, join_query, join_fragment),
                )
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
            path = self._normalize_path(path) if "." in path else path

        return self._from_val(
            tuple.__new__(SplitResult, (scheme, orig_netloc, path, query, fragment))
        )

    def joinpath(self, *other: str, encoded: bool = False) -> "URL":
        """Return a new URL with the elements in other appended to the path."""
        return self._make_child(other, encoded=encoded)

    def human_repr(self) -> str:
        """Return decoded human readable string for URL representation."""
        user = _human_quote(self.user, "#/:?@[]")
        password = _human_quote(self.password, "#/:?@[]")
        if (host := self.host) and ":" in host:
            host = f"[{host}]"
        path = _human_quote(self.path, "#?")
        if TYPE_CHECKING:
            assert path is not None
        query_string = "&".join(
            "{}={}".format(_human_quote(k, "#&+;="), _human_quote(v, "#&+;="))
            for k, v in self.query.items()
        )
        fragment = _human_quote(self.fragment, "")
        if TYPE_CHECKING:
            assert fragment is not None
        netloc = self._make_netloc(user, password, host, self.explicit_port)
        scheme = self._val.scheme
        return self._unsplit_result(scheme, netloc, path, query_string, fragment)


def _human_quote(s: Union[str, None], unsafe: str) -> Union[str, None]:
    if not s:
        return s
    for c in "%" + unsafe:
        if c in s:
            s = s.replace(c, f"%{ord(c):02X}")
    if s.isprintable():
        return s
    return "".join(c if c.isprintable() else quote(c) for c in s)


_MAXCACHE = 256


@lru_cache(_MAXCACHE)
def _idna_decode(raw: str) -> str:
    try:
        return idna.decode(raw.encode("ascii"))
    except UnicodeError:  # e.g. '::1'
        return raw.encode("ascii").decode("idna")


@lru_cache(_MAXCACHE)
def _idna_encode(host: str) -> str:
    try:
        return idna.encode(host, uts46=True).decode("ascii")
    except UnicodeError:
        return host.encode("idna").decode("ascii")


@lru_cache(_MAXCACHE)
def _ip_compressed_version(raw_ip: str) -> tuple[str, int]:
    """Return compressed version of IP address and its version."""
    ip = ip_address(raw_ip)
    return ip.compressed, ip.version


@lru_cache(_MAXCACHE)
def _host_validate(host: str) -> None:
    """Validate an ascii host name."""
    invalid = _not_reg_name.search(host)
    if invalid is None:
        return
    value, pos, extra = invalid.group(), invalid.start(), ""
    if value == "@" or (value == ":" and "@" in host[pos:]):
        # this looks like an authority string
        extra = (
            ", if the value includes a username or password, "
            "use 'authority' instead of 'host'"
        )
    raise ValueError(
        f"Host {host!r} cannot contain {value!r} (at position " f"{pos}){extra}"
    ) from None


@rewrite_module
def cache_clear() -> None:
    """Clear all LRU caches."""
    _idna_decode.cache_clear()
    _idna_encode.cache_clear()
    _ip_compressed_version.cache_clear()
    _host_validate.cache_clear()


@rewrite_module
def cache_info() -> CacheInfo:
    """Report cache statistics."""
    return {
        "idna_encode": _idna_encode.cache_info(),
        "idna_decode": _idna_decode.cache_info(),
        "ip_address": _ip_compressed_version.cache_info(),
        "host_validate": _host_validate.cache_info(),
    }


@rewrite_module
def cache_configure(
    *,
    idna_encode_size: Union[int, None] = _MAXCACHE,
    idna_decode_size: Union[int, None] = _MAXCACHE,
    ip_address_size: Union[int, None] = _MAXCACHE,
    host_validate_size: Union[int, None] = _MAXCACHE,
) -> None:
    """Configure LRU cache sizes."""
    global _idna_decode, _idna_encode, _ip_compressed_version, _host_validate

    _idna_encode = lru_cache(idna_encode_size)(_idna_encode.__wrapped__)
    _idna_decode = lru_cache(idna_decode_size)(_idna_decode.__wrapped__)
    _ip_compressed_version = lru_cache(ip_address_size)(
        _ip_compressed_version.__wrapped__
    )
    _host_validate = lru_cache(host_validate_size)(_host_validate.__wrapped__)
