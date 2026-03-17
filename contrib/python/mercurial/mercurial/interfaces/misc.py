# misc.py - Various Interface that did not deserve a dedicated module (yet)
#
# Copyright 2025 Octobus, contact@octobus.net
from __future__ import annotations

import abc
import os

from typing import (
    Any,
    Callable,
    Iterator,
    Optional,
    Protocol,
)


class IHooks(Protocol):
    """A collection of hook functions that can be used to extend a
    function's behavior. Hooks are called in lexicographic order,
    based on the names of their sources."""

    @abc.abstractmethod
    def add(self, source: bytes, hook: Callable):
        ...

    @abc.abstractmethod
    def __call__(self, *args) -> list:
        ...


class IDirs(Protocol):
    '''a multiset of directory names from a set of file paths'''

    @abc.abstractmethod
    def addpath(self, path: bytes) -> None:
        ...

    @abc.abstractmethod
    def delpath(self, path: bytes) -> None:
        ...

    @abc.abstractmethod
    def __iter__(self) -> Iterator[bytes]:
        ...

    @abc.abstractmethod
    def __contains__(self, d: bytes) -> bool:
        ...


class ICacheStat(Protocol):
    stat: os.stat_result

    @abc.abstractmethod
    def cacheable(self) -> bool:
        ...

    @abc.abstractmethod
    def __hash__(self) -> int:
        ...

    @abc.abstractmethod
    def __eq__(self, other: Any) -> bool:
        ...

    @abc.abstractmethod
    def __ne__(self, other: Any) -> bool:
        ...


AuthInfoT = tuple[
    bytes,
    Optional[
        tuple[
            None,
            tuple[bytes, bytes],
            bytes,
            bytes,
        ]
    ],
]


class IUrl(Protocol):
    r"""Reliable URL parser.

    This parses URLs and provides attributes for the following
    components:

    <scheme>://<user>:<passwd>@<host>:<port>/<path>?<query>#<fragment>

    Missing components are set to None. The only exception is
    fragment, which is set to '' if present but empty.

    If parsefragment is False, fragment is included in query. If
    parsequery is False, query is included in path. If both are
    False, both fragment and query are included in path.

    See http://www.ietf.org/rfc/rfc2396.txt for more information.
    """

    path: bytes | None
    scheme: bytes | None
    user: bytes | None
    passwd: bytes | None
    host: bytes | None
    port: bytes | None
    query: bytes | None
    fragment: bytes | None

    @abc.abstractmethod
    def copy(self) -> IUrl:
        ...

    @abc.abstractmethod
    def authinfo(self) -> AuthInfoT:
        ...

    @abc.abstractmethod
    def isabs(self) -> bool:
        ...

    @abc.abstractmethod
    def localpath(self) -> bytes:
        ...

    @abc.abstractmethod
    def islocal(self) -> bool:
        ...


class IPath(Protocol):
    """Represents an individual path and its configuration."""

    name: bytes
    main_path: IPath | None
    url: IUrl
    raw_url: IUrl
    branch: bytes
    rawloc: bytes
    loc: bytes

    @abc.abstractmethod
    def copy(self, new_raw_location: bytes | None = None) -> IPath:
        ...

    @property
    @abc.abstractmethod
    def is_push_variant(self) -> bool:
        """is this a path variant to be used for pushing"""

    @abc.abstractmethod
    def get_push_variant(self) -> IPath:
        """get a "copy" of the path, but suitable for pushing

        This means using the value of the `pushurl` option (if any) as the url.

        The original path is available in the `main_path` attribute.
        """

    @property
    @abc.abstractmethod
    def suboptions(self) -> dict[bytes, bytes]:
        """Return sub-options and their values for this path.

        This is intended to be used for presentation purposes.
        """
