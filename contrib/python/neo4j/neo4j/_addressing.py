# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

from contextlib import suppress as _suppress
from socket import (
    AddressFamily,
    AF_INET,
    AF_INET6,
    getservbyname,
)

from . import _typing as t


_T = t.TypeVar("_T")


if t.TYPE_CHECKING:

    class _WithPeerName(t.Protocol):
        def getpeername(self) -> tuple: ...


__all__ = [
    "Address",
    "IPv4Address",
    "IPv6Address",
    "ResolvedAddress",
    "ResolvedIPv4Address",
    "ResolvedIPv6Address",
]


class _AddressMeta(type(tuple)):  # type: ignore[misc]
    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls._ipv4_cls = None
        cls._ipv6_cls = None

    def _subclass_by_family(cls, family):
        subclasses = [
            sc
            for sc in cls.__subclasses__()
            if (
                sc.__module__ == cls.__module__
                and getattr(sc, "family", None) == family
            )
        ]
        if len(subclasses) != 1:
            raise ValueError(
                f"Class {cls} needs exactly one direct subclass with "
                f"attribute `family == {family}` within this module. "
                f"Found: {subclasses}"
            )
        return subclasses[0]

    @property
    def ipv4_cls(cls):
        if cls._ipv4_cls is None:
            cls._ipv4_cls = cls._subclass_by_family(AF_INET)
        return cls._ipv4_cls

    @property
    def ipv6_cls(cls):
        if cls._ipv6_cls is None:
            cls._ipv6_cls = cls._subclass_by_family(AF_INET6)
        return cls._ipv6_cls


class Address(tuple, metaclass=_AddressMeta):
    """
    Base class to represent server addresses within the driver.

    A tuple of two (IPv4) or four (IPv6) elements, representing the address
    parts. See also python's :mod:`socket` module for more information.

        >>> Address(("example.com", 7687))
        IPv4Address(('example.com', 7687))
        >>> Address(("127.0.0.1", 7687))
        IPv4Address(('127.0.0.1', 7687))
        >>> Address(("::1", 7687, 0, 0))
        IPv6Address(('::1', 7687, 0, 0))

    :param iterable: A collection of two or four elements creating an
        :class:`.IPv4Address` or :class:`.IPv6Address` instance respectively.
    """

    #: Address family (:data:`socket.AF_INET` or :data:`socket.AF_INET6`).
    family: AddressFamily | None = None

    def __new__(cls, iterable: t.Collection) -> Address:
        if isinstance(iterable, cls):
            return iterable
        n_parts = len(iterable)
        inst = tuple.__new__(cls, iterable)
        if n_parts == 2:
            inst.__class__ = cls.ipv4_cls
        elif n_parts == 4:
            inst.__class__ = cls.ipv6_cls
        else:
            raise ValueError(
                "Addresses must consist of either "
                "two parts (IPv4) or four parts (IPv6)"
            )
        return inst

    @classmethod
    def from_socket(cls, socket: _WithPeerName) -> Address:
        """
        Create an address from a socket object.

        Uses the socket's ``getpeername`` method to retrieve the remote
        address the socket is connected to.
        """
        address = socket.getpeername()
        return cls(address)

    @classmethod
    def parse(
        cls,
        s: str,
        default_host: str | None = None,
        default_port: int | None = None,
    ) -> Address:
        """
        Parse a string into an address.

        The string must be in the format ``host:port`` (IPv4) or
        ``[host]:port`` (IPv6).
        If no port is specified, or is empty, ``default_port`` will be used.
        If no host is specified, or is empty, ``default_host`` will be used.

            >>> Address.parse("localhost:7687")
            IPv4Address(('localhost', 7687))
            >>> Address.parse("[::1]:7687")
            IPv6Address(('::1', 7687, 0, 0))
            >>> Address.parse("localhost")
            IPv4Address(('localhost', 0))
            >>> Address.parse("localhost", default_port=1234)
            IPv4Address(('localhost', 1234))

        :param s: The string to parse.
        :param default_host: The default host to use if none is specified.
            :data:`None` indicates to use ``"localhost"`` as default.
        :param default_port: The default port to use if none is specified.
            :data:`None` indicates to use ``0`` as default.

        :returns: The parsed address.
        """
        if not isinstance(s, str):
            raise TypeError("Address.parse requires a string argument")
        if s.startswith("["):
            # IPv6
            port: str | int
            host, _, port = s[1:].rpartition("]")
            port = port.lstrip(":")
            with _suppress(TypeError, ValueError):
                port = int(port)
            host = host or default_host or "localhost"
            port = port or default_port or 0
            return cls((host, port, 0, 0))
        else:
            # IPv4
            host, _, port = s.partition(":")
            with _suppress(TypeError, ValueError):
                port = int(port)
            host = host or default_host or "localhost"
            port = port or default_port or 0
            return cls((host, port))

    @classmethod
    def parse_list(
        cls,
        *s: str,
        default_host: str | None = None,
        default_port: int | None = None,
    ) -> list[Address]:
        """
        Parse multiple addresses into a list.

        See :meth:`.parse` for details on the string format.

        Either a whitespace-separated list of strings or multiple strings
        can be used.

            >>> Address.parse_list("localhost:7687", "[::1]:7687")
            [IPv4Address(('localhost', 7687)), IPv6Address(('::1', 7687, 0, 0))]
            >>> Address.parse_list("localhost:7687 [::1]:7687")
            [IPv4Address(('localhost', 7687)), IPv6Address(('::1', 7687, 0, 0))]

        :param s: The string(s) to parse.
        :param default_host: The default host to use if none is specified.
            :data:`None` indicates to use ``"localhost"`` as default.
        :param default_port: The default port to use if none is specified.
            :data:`None` indicates to use ``0`` as default.

        :returns: The list of parsed addresses.
        """  # noqa: E501 can't split the doctest lines
        if not all(isinstance(s0, str) for s0 in s):
            raise TypeError("Address.parse_list requires a string argument")
        return [
            cls.parse(a, default_host, default_port)
            for a in " ".join(s).split()
        ]

    def __repr__(self):
        return f"{self.__class__.__name__}({tuple(self)!r})"

    @property
    def _host_name(self) -> t.Any:
        return self[0]

    @property
    def host(self) -> t.Any:
        """
        The host part of the address.

        This is the first part of the address tuple.

            >>> Address(("localhost", 7687)).host
            'localhost'
        """
        return self[0]

    @property
    def port(self) -> t.Any:
        """
        The port part of the address.

        This is the second part of the address tuple.

            >>> Address(("localhost", 7687)).port
            7687
            >>> Address(("localhost", 7687, 0, 0)).port
            7687
            >>> Address(("localhost", "7687")).port
            '7687'
            >>> Address(("localhost", "http")).port
            'http'
        """
        return self[1]

    @property
    def _unresolved(self) -> Address:
        return self

    @property
    def port_number(self) -> int:
        """
        The port part of the address as an integer.

        First try to resolve the port as an integer, using
        :func:`socket.getservbyname`. If that fails, fall back to parsing the
        port as an integer.

            >>> Address(("localhost", 7687)).port_number
            7687
            >>> Address(("localhost", "http")).port_number
            80
            >>> Address(("localhost", "7687")).port_number
            7687
            >>> Address(("localhost", [])).port_number
            Traceback (most recent call last):
                ...
            TypeError: Unknown port value []
            >>> Address(("localhost", "banana-protocol")).port_number
            Traceback (most recent call last):
                ...
            ValueError: Unknown port value 'banana-protocol'

        :returns: The resolved port number.

        :raise ValueError: If the port cannot be resolved.
        :raise TypeError: If the port cannot be resolved.
        """
        error_cls: type = TypeError

        try:
            return getservbyname(self[1])
        except OSError:
            # OSError: service/proto not found
            error_cls = ValueError
        except TypeError:
            # TypeError: getservbyname() argument 1 must be str, not X
            pass
        try:
            return int(self[1])
        except ValueError:
            error_cls = ValueError
        except TypeError:
            pass
        raise error_cls(f"Unknown port value {self[1]!r}")

    def __reduce__(self):
        return Address, (tuple(self),)


class IPv4Address(Address):
    """
    An IPv4 address (family ``AF_INET``).

    This class is also used for addresses that specify a host name instead of
    an IP address. E.g.,

        >>> Address(("example.com", 7687))
        IPv4Address(('example.com', 7687))

    This class should not be instantiated directly. Instead, use
    :class:`.Address` or one of its factory methods.
    """

    family = AF_INET

    def __str__(self) -> str:
        return "{}:{}".format(*self)


class IPv6Address(Address):
    """
    An IPv6 address (family ``AF_INET6``).

    This class should not be instantiated directly. Instead, use
    :class:`.Address` or one of its factory methods.
    """

    family = AF_INET6

    def __str__(self) -> str:
        return "[{}]:{}".format(*self)


class ResolvedAddress(Address):
    _unresolved_host_name: str

    @property
    def _host_name(self) -> str:
        return self._unresolved_host_name

    @property
    def _unresolved(self) -> Address:
        return super().__new__(Address, (self._host_name, *self[1:]))

    def __new__(cls, iterable, host_name: str) -> ResolvedAddress:
        new = super().__new__(cls, iterable)
        new = t.cast(ResolvedAddress, new)
        new._unresolved_host_name = host_name
        return new

    def __reduce__(self):
        return ResolvedAddress, (tuple(self), self._unresolved_host_name)


class ResolvedIPv4Address(IPv4Address, ResolvedAddress):
    pass


class ResolvedIPv6Address(IPv6Address, ResolvedAddress):
    pass
