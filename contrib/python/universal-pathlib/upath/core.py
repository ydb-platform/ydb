from __future__ import annotations

import os
import sys
import warnings
from copy import copy
from pathlib import Path
from types import MappingProxyType
from typing import IO
from typing import TYPE_CHECKING
from typing import Any
from typing import BinaryIO
from typing import Generator
from typing import Literal
from typing import Mapping
from typing import Sequence
from typing import TextIO
from typing import TypeVar
from typing import overload
from urllib.parse import urlsplit

from fsspec.registry import get_filesystem_class
from fsspec.spec import AbstractFileSystem

from upath._compat import FSSpecAccessorShim
from upath._compat import PathlibPathShim
from upath._compat import method_and_classmethod
from upath._compat import str_remove_prefix
from upath._compat import str_remove_suffix
from upath._flavour import LazyFlavourDescriptor
from upath._flavour import upath_get_kwargs_from_url
from upath._flavour import upath_urijoin
from upath._protocol import compatible_protocol
from upath._protocol import get_upath_protocol
from upath._stat import UPathStatResult
from upath.registry import get_upath_class

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    if sys.version_info >= (3, 11):
        from typing import Self
    else:
        from typing_extensions import Self

__all__ = ["UPath"]


def __getattr__(name):
    if name == "_UriFlavour":
        from upath._flavour import default_flavour

        warnings.warn(
            "upath.core._UriFlavour should not be used anymore."
            " Please follow the universal_pathlib==0.2.0 migration guide at"
            " https://github.com/fsspec/universal_pathlib for more"
            " information.",
            DeprecationWarning,
            stacklevel=2,
        )
        return default_flavour
    elif name == "PT":
        warnings.warn(
            "upath.core.PT should not be used anymore."
            " Please follow the universal_pathlib==0.2.0 migration guide at"
            " https://github.com/fsspec/universal_pathlib for more"
            " information.",
            DeprecationWarning,
            stacklevel=2,
        )
        return TypeVar("PT", bound="UPath")
    else:
        raise AttributeError(name)


_FSSPEC_HAS_WORKING_GLOB = None


def _check_fsspec_has_working_glob():
    global _FSSPEC_HAS_WORKING_GLOB
    from fsspec.implementations.memory import MemoryFileSystem

    m = type("_M", (MemoryFileSystem,), {"store": {}, "pseudo_dirs": [""]})()
    m.touch("a.txt")
    m.touch("f/b.txt")
    g = _FSSPEC_HAS_WORKING_GLOB = len(m.glob("**/*.txt")) == 2
    return g


def _make_instance(cls, args, kwargs):
    """helper for pickling UPath instances"""
    return cls(*args, **kwargs)


_unset: Any = object()


# accessors are deprecated
_FSSpecAccessor = FSSpecAccessorShim


class UPath(PathlibPathShim, Path):
    __slots__ = (
        "_protocol",
        "_storage_options",
        "_fs_cached",
        *PathlibPathShim.__missing_py312_slots__,
        "__drv",
        "__root",
        "__parts",
    )

    if TYPE_CHECKING:
        # public
        anchor: str
        drive: str
        parent: Self
        parents: Sequence[Self]
        parts: tuple[str, ...]
        root: str
        stem: str
        suffix: str
        suffixes: list[str]

        def with_name(self, name: str) -> Self: ...
        def with_stem(self, stem: str) -> Self: ...
        def with_suffix(self, suffix: str) -> Self: ...

        # private attributes
        _protocol: str
        _storage_options: dict[str, Any]
        _fs_cached: AbstractFileSystem
        _tail: str

    _protocol_dispatch: bool | None = None
    _flavour = LazyFlavourDescriptor()

    if sys.version_info >= (3, 13):
        parser = _flavour

    # === upath.UPath constructor =====================================

    def __new__(
        cls, *args, protocol: str | None = None, **storage_options: Any
    ) -> UPath:
        # fill empty arguments
        if not args:
            args = (".",)

        # create a copy if UPath class
        part0, *parts = args
        if not parts and not storage_options and isinstance(part0, cls):
            return copy(part0)

        # deprecate 'scheme'
        if "scheme" in storage_options:
            warnings.warn(
                "use 'protocol' kwarg instead of 'scheme'",
                DeprecationWarning,
                stacklevel=2,
            )
            protocol = storage_options.pop("scheme")

        # determine the protocol
        pth_protocol = get_upath_protocol(
            part0, protocol=protocol, storage_options=storage_options
        )
        # determine which UPath subclass to dispatch to
        if cls._protocol_dispatch or cls._protocol_dispatch is None:
            upath_cls = get_upath_class(protocol=pth_protocol)
            if upath_cls is None:
                raise ValueError(f"Unsupported filesystem: {pth_protocol!r}")
        else:
            # user subclasses can request to disable protocol dispatch
            # by setting MyUPathSubclass._protocol_dispatch to `False`.
            # This will effectively ignore the registered UPath
            # implementations and return an instance of MyUPathSubclass.
            # This can be useful if a subclass wants to extend the UPath
            # api, and it is fine to rely on the default implementation
            # for all supported user protocols.
            upath_cls = cls

        # create a new instance
        if cls is UPath:
            # we called UPath() directly, and want an instance based on the
            # provided or detected protocol (i.e. upath_cls)
            obj: UPath = object.__new__(upath_cls)
            obj._protocol = pth_protocol

        elif issubclass(cls, upath_cls):
            # we called a sub- or sub-sub-class of UPath, i.e. S3Path() and the
            # corresponding upath_cls based on protocol is equal-to or a
            # parent-of the cls.
            obj = object.__new__(cls)
            obj._protocol = pth_protocol

        elif issubclass(cls, UPath):
            # we called a subclass of UPath directly, i.e. S3Path() but the
            # detected protocol would return a non-related UPath subclass, i.e.
            # S3Path("file:///abc"). This behavior is going to raise an error
            # in future versions
            msg_protocol = repr(pth_protocol)
            if not pth_protocol:
                msg_protocol += " (empty string)"
            msg = (
                f"{cls.__name__!s}(...) detected protocol {msg_protocol!s} and"
                f" returns a {upath_cls.__name__} instance that isn't a direct"
                f" subclass of {cls.__name__}. This will raise an exception in"
                " future universal_pathlib versions. To prevent the issue, use"
                " UPath(...) to create instances of unrelated protocols or you"
                f" can instead derive your subclass {cls.__name__!s}(...) from"
                f" {upath_cls.__name__} or alternatively override behavior via"
                f" registering the {cls.__name__} implementation with protocol"
                f" {msg_protocol!s} replacing the default implementation."
            )
            warnings.warn(msg, DeprecationWarning, stacklevel=2)

            obj = object.__new__(upath_cls)
            obj._protocol = pth_protocol

            upath_cls.__init__(
                obj, *args, protocol=pth_protocol, **storage_options
            )  # type: ignore

        else:
            raise RuntimeError("UPath.__new__ expected cls to be subclass of UPath")

        return obj

    def __init__(
        self, *args, protocol: str | None = None, **storage_options: Any
    ) -> None:
        # allow subclasses to customize __init__ arg parsing
        base_options = getattr(self, "_storage_options", {})
        args, protocol, storage_options = type(self)._transform_init_args(
            args, protocol or self._protocol, {**base_options, **storage_options}
        )
        if self._protocol != protocol and protocol:
            self._protocol = protocol

        # retrieve storage_options
        if args:
            args0 = args[0]
            if isinstance(args0, UPath):
                self._storage_options = {**args0.storage_options, **storage_options}
            else:
                if hasattr(args0, "__fspath__"):
                    _args0 = args0.__fspath__()
                else:
                    _args0 = str(args0)
                self._storage_options = type(self)._parse_storage_options(
                    _args0, protocol, storage_options
                )
        else:
            self._storage_options = storage_options.copy()

        # check that UPath subclasses in args are compatible
        # TODO:
        #   Future versions of UPath could verify that storage_options
        #   can be combined between UPath instances. Not sure if this
        #   is really necessary though. A warning might be enough...
        if not compatible_protocol(self._protocol, *args):
            raise ValueError("can't combine incompatible UPath protocols")

        # fill ._raw_paths
        if hasattr(self, "_raw_paths"):
            return
        super().__init__(*args)

    # === upath.UPath PUBLIC ADDITIONAL API ===========================

    @property
    def protocol(self) -> str:
        """The fsspec protocol for the path."""
        return self._protocol

    @property
    def storage_options(self) -> Mapping[str, Any]:
        """The fsspec storage options for the path."""
        return MappingProxyType(self._storage_options)

    @property
    def fs(self) -> AbstractFileSystem:
        """The cached fsspec filesystem instance for the path."""
        try:
            return self._fs_cached
        except AttributeError:
            fs = self._fs_cached = self._fs_factory(
                str(self), self.protocol, self.storage_options
            )
            return fs

    @property
    def path(self) -> str:
        """The path that a fsspec filesystem can use."""
        return super().__str__()

    def joinuri(self, uri: str | os.PathLike[str]) -> UPath:
        """Join with urljoin behavior for UPath instances"""
        # short circuit if the new uri uses a different protocol
        other_protocol = get_upath_protocol(uri)
        if other_protocol and other_protocol != self._protocol:
            return UPath(uri)
        return UPath(
            upath_urijoin(str(self), str(uri)),
            protocol=other_protocol or self._protocol,
            **self.storage_options,
        )

    # === upath.UPath CUSTOMIZABLE API ================================

    @classmethod
    def _transform_init_args(
        cls,
        args: tuple[str | os.PathLike, ...],
        protocol: str,
        storage_options: dict[str, Any],
    ) -> tuple[tuple[str | os.PathLike, ...], str, dict[str, Any]]:
        """allow customization of init args in subclasses"""
        return args, protocol, storage_options

    @classmethod
    def _parse_storage_options(
        cls, urlpath: str, protocol: str, storage_options: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Parse storage_options from the urlpath"""
        pth_storage_options = upath_get_kwargs_from_url(urlpath)
        return {**pth_storage_options, **storage_options}

    @classmethod
    def _fs_factory(
        cls, urlpath: str, protocol: str, storage_options: Mapping[str, Any]
    ) -> AbstractFileSystem:
        """Instantiate the filesystem_spec filesystem class"""
        fs_cls = get_filesystem_class(protocol)
        so_dct = fs_cls._get_kwargs_from_urls(urlpath)
        so_dct.update(storage_options)
        return fs_cls(**storage_options)

    # === upath.UPath COMPATIBILITY API ===============================

    def __init_subclass__(cls, **kwargs):
        """provide a clean migration path for custom user subclasses"""

        # Check if the user subclass has a custom `__new__` method
        has_custom_new_method = (
            cls.__new__ is not UPath.__new__
            and cls.__name__ not in {"PosixUPath", "WindowsUPath"}
        )

        if has_custom_new_method and cls._protocol_dispatch is None:
            warnings.warn(
                "Detected a customized `__new__` method in subclass"
                f" {cls.__name__!r}. Protocol dispatch will be disabled"
                " for this subclass. Please follow the"
                " universal_pathlib==0.2.0 migration guide at"
                " https://github.com/fsspec/universal_pathlib for more"
                " information.",
                DeprecationWarning,
                stacklevel=2,
            )
            cls._protocol_dispatch = False

        # Check if the user subclass has defined a custom accessor class
        accessor_cls = getattr(cls, "_default_accessor", None)

        has_custom_legacy_accessor = (
            accessor_cls is not None
            and issubclass(accessor_cls, FSSpecAccessorShim)
            and accessor_cls is not FSSpecAccessorShim
        )
        has_customized_fs_instantiation = (
            accessor_cls.__init__ is not FSSpecAccessorShim.__init__
            or hasattr(accessor_cls, "_fs")
        )

        if has_custom_legacy_accessor and has_customized_fs_instantiation:
            warnings.warn(
                "Detected a customized `__init__` method or `_fs` attribute"
                f" in the provided `_FSSpecAccessor` subclass of {cls.__name__!r}."
                " It is recommended to instead override the `UPath._fs_factory`"
                " classmethod to customize filesystem instantiation. Please follow"
                " the universal_pathlib==0.2.0 migration guide at"
                " https://github.com/fsspec/universal_pathlib for more"
                " information.",
                DeprecationWarning,
                stacklevel=2,
            )

            def _fs_factory(
                cls_, urlpath: str, protocol: str, storage_options: Mapping[str, Any]
            ) -> AbstractFileSystem:
                url = urlsplit(urlpath)
                if protocol:
                    url = url._replace(scheme=protocol)
                inst = cls_._default_accessor(url, **storage_options)
                return inst._fs

            def _parse_storage_options(
                cls_, urlpath: str, protocol: str, storage_options: Mapping[str, Any]
            ) -> dict[str, Any]:
                url = urlsplit(urlpath)
                if protocol:
                    url = url._replace(scheme=protocol)
                inst = cls_._default_accessor(url, **storage_options)
                return inst._fs.storage_options

            cls._fs_factory = classmethod(_fs_factory)
            cls._parse_storage_options = classmethod(_parse_storage_options)

    @property
    def _path(self):
        warnings.warn(
            "UPath._path is deprecated and should not be used."
            " Please follow the universal_pathlib==0.2.0 migration guide at"
            " https://github.com/fsspec/universal_pathlib for more"
            " information.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.path

    @property
    def _kwargs(self):
        warnings.warn(
            "UPath._kwargs is deprecated. Please use"
            " UPath.storage_options instead. Follow the"
            " universal_pathlib==0.2.0 migration guide at"
            " https://github.com/fsspec/universal_pathlib for more"
            " information.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.storage_options

    @property
    def _url(self) -> SplitResult:
        # TODO:
        #   _url should be deprecated, but for now there is no good way of
        #   accessing query parameters from urlpaths...
        return urlsplit(self.as_posix())

    if not TYPE_CHECKING:
        # allow mypy to catch missing attributes

        def __getattr__(self, item):
            if item == "_accessor":
                warnings.warn(
                    "UPath._accessor is deprecated. Please use"
                    " UPath.fs instead. Follow the"
                    " universal_pathlib==0.2.0 migration guide at"
                    " https://github.com/fsspec/universal_pathlib for more"
                    " information.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                if hasattr(self, "_default_accessor"):
                    accessor_cls = self._default_accessor
                else:
                    accessor_cls = FSSpecAccessorShim
                return accessor_cls.from_path(self)
            else:
                raise AttributeError(item)

    @classmethod
    def _from_parts(cls, parts, **kwargs):
        warnings.warn(
            "UPath._from_parts is deprecated and should not be used."
            " Please follow the universal_pathlib==0.2.0 migration guide at"
            " https://github.com/fsspec/universal_pathlib for more"
            " information.",
            DeprecationWarning,
            stacklevel=2,
        )
        parsed_url = kwargs.pop("url", None)
        if parsed_url:
            if protocol := parsed_url.scheme:
                kwargs["protocol"] = protocol
            if netloc := parsed_url.netloc:
                kwargs["netloc"] = netloc
        obj = UPath.__new__(cls, parts, **kwargs)
        obj.__init__(*parts, **kwargs)
        return obj

    @classmethod
    def _parse_args(cls, args):
        warnings.warn(
            "UPath._parse_args is deprecated and should not be used."
            " Please follow the universal_pathlib==0.2.0 migration guide at"
            " https://github.com/fsspec/universal_pathlib for more"
            " information.",
            DeprecationWarning,
            stacklevel=2,
        )
        # TODO !!!
        pth = cls._flavour.join(*args)
        return cls._parse_path(pth)

    @property
    def _drv(self):
        # direct access to ._drv should emit a warning,
        # but there is no good way of doing this for now...
        try:
            return self.__drv
        except AttributeError:
            self._load_parts()
            return self.__drv

    @_drv.setter
    def _drv(self, value):
        self.__drv = value

    @property
    def _root(self):
        # direct access to ._root should emit a warning,
        # but there is no good way of doing this for now...
        try:
            return self.__root
        except AttributeError:
            self._load_parts()
            return self.__root

    @_root.setter
    def _root(self, value):
        self.__root = value

    @property
    def _parts(self):
        # UPath._parts is not used anymore, and not available
        # in pathlib.Path for Python 3.12 and later.
        # Direct access to ._parts should emit a deprecation warning,
        # but there is no good way of doing this for now...
        try:
            return self.__parts
        except AttributeError:
            self._load_parts()
            self.__parts = super().parts
            return list(self.__parts)

    @_parts.setter
    def _parts(self, value):
        self.__parts = value

    @property
    def _cparts(self):
        # required for pathlib.Path.__eq__ compatibility on Python <3.12
        return self.parts

    # === pathlib.PurePath ============================================

    def __reduce__(self):
        args = tuple(self._raw_paths)
        kwargs = {
            "protocol": self._protocol,
            **self._storage_options,
        }
        return _make_instance, (type(self), args, kwargs)

    def with_segments(self, *pathsegments: str | os.PathLike[str]) -> Self:
        return type(self)(
            *pathsegments,
            protocol=self._protocol,
            **self._storage_options,
        )

    def joinpath(self, *pathsegments: str | os.PathLike[str]) -> Self:
        return self.with_segments(self, *pathsegments)

    def __truediv__(self, key: str | os.PathLike[str]) -> Self:
        try:
            return self.joinpath(key)
        except TypeError:
            return NotImplemented

    def __rtruediv__(self, key: str | os.PathLike[str]) -> Self:
        try:
            return self.with_segments(key, self)
        except TypeError:
            return NotImplemented

    # === upath.UPath non-standard changes ============================

    # NOTE:
    #  this is a classmethod on the parent class, but we need to
    #  override it here to make it possible to provide the _flavour
    #  with the correct protocol...
    #  pathlib 3.12 never calls this on the class. Only on the instance.
    @method_and_classmethod
    def _parse_path(self_or_cls, path):  # noqa: B902
        if isinstance(self_or_cls, type):
            warnings.warn(
                "UPath._parse_path should not be used as a classmethod."
                " Please file an issue on the universal_pathlib issue tracker"
                " and describe your use case.",
                DeprecationWarning,
                stacklevel=2,
            )
        flavour = self_or_cls._flavour

        if flavour.supports_empty_parts:
            drv, root, rel = flavour.splitroot(path)
            if not root:
                parsed = []
            else:
                parsed = list(map(sys.intern, rel.split(flavour.sep)))
                if parsed[-1] == ".":
                    parsed[-1] = ""
                parsed = [x for x in parsed if x != "."]
                if not flavour.has_meaningful_trailing_slash and parsed[-1] == "":
                    parsed.pop()
            return drv, root, parsed
        if not path:
            return "", "", []
        sep = flavour.sep
        altsep = flavour.altsep
        if altsep:
            path = path.replace(altsep, sep)
        drv, root, rel = flavour.splitroot(path)
        if not root and drv.startswith(sep) and not drv.endswith(sep):
            drv_parts = drv.split(sep)
            if len(drv_parts) == 4 and drv_parts[2] not in "?.":
                # e.g. //server/share
                root = sep
            elif len(drv_parts) == 6:
                # e.g. //?/unc/server/share
                root = sep
        parsed = [sys.intern(str(x)) for x in rel.split(sep) if x and x != "."]
        return drv, root, parsed

    @method_and_classmethod
    def _format_parsed_parts(self_or_cls, drv, root, tail, **kwargs):  # noqa: B902
        if isinstance(self_or_cls, type):
            warnings.warn(
                "UPath._format_parsed_path should not be used as a classmethod."
                " Please file an issue on the universal_pathlib issue tracker"
                " and describe your use case.",
                DeprecationWarning,
                stacklevel=2,
            )
        flavour = self_or_cls._flavour

        if kwargs:
            warnings.warn(
                "UPath._format_parsed_parts should not be used with"
                " additional kwargs. Please follow the"
                " universal_pathlib==0.2.0 migration guide at"
                " https://github.com/fsspec/universal_pathlib for more"
                " information.",
                DeprecationWarning,
                stacklevel=2,
            )
            if "url" in kwargs and tail[:1] == [f"{drv}{root}"]:
                # This was called from code that expected py38-py311 behavior
                # of _format_parsed_parts, which takes drv, root and parts
                tail = tail[1:]

        if drv or root:
            return drv + root + flavour.sep.join(tail)
        elif tail and flavour.splitdrive(tail[0])[0]:
            tail = ["."] + tail
        return flavour.sep.join(tail)

    # === upath.UPath changes =========================================

    def __str__(self):
        if self._protocol:
            return f"{self._protocol}://{self.path}"
        else:
            return self.path

    def __fspath__(self):
        msg = (
            "in a future version of UPath this will be set to None"
            " unless the filesystem is local (or caches locally)"
        )
        warnings.warn(msg, PendingDeprecationWarning, stacklevel=2)
        return str(self)

    def __bytes__(self):
        msg = (
            "in a future version of UPath this will be set to None"
            " unless the filesystem is local (or caches locally)"
        )
        warnings.warn(msg, PendingDeprecationWarning, stacklevel=2)
        return os.fsencode(self)

    def as_uri(self) -> str:
        return str(self)

    def is_reserved(self) -> bool:
        return False

    def __eq__(self, other: object) -> bool:
        """UPaths are considered equal if their protocol, path and
        storage_options are equal."""
        if not isinstance(other, UPath):
            return NotImplemented
        return (
            self.path == other.path
            and self.protocol == other.protocol
            and self.storage_options == other.storage_options
        )

    def __hash__(self) -> int:
        """The returned hash is based on the protocol and path only.

        Note: in the future, if hash collisions become an issue, we
          can add `fsspec.utils.tokenize(storage_options)`
        """
        return hash((self.protocol, self.path))

    def relative_to(  # type: ignore[override]
        self,
        other,
        /,
        *_deprecated,
        walk_up=False,
    ) -> Self:
        if isinstance(other, UPath) and self.storage_options != other.storage_options:
            raise ValueError(
                "paths have different storage_options:"
                f" {self.storage_options!r} != {other.storage_options!r}"
            )
        return super().relative_to(other, *_deprecated, walk_up=walk_up)

    def is_relative_to(self, other, /, *_deprecated) -> bool:  # type: ignore[override]
        if isinstance(other, UPath) and self.storage_options != other.storage_options:
            return False
        return super().is_relative_to(other, *_deprecated)

    @property
    def name(self) -> str:
        tail = self._tail
        if not tail:
            return ""
        name = tail[-1]
        if not name and len(tail) >= 2:
            return tail[-2]
        else:
            return name

    # === pathlib.Path ================================================

    def stat(  # type: ignore[override]
        self,
        *,
        follow_symlinks=True,
    ) -> UPathStatResult:
        if not follow_symlinks:
            warnings.warn(
                f"{type(self).__name__}.stat(follow_symlinks=False):"
                " is currently ignored.",
                UserWarning,
                stacklevel=2,
            )
        return UPathStatResult.from_info(self.fs.stat(self.path))

    def lstat(self) -> UPathStatResult:  # type: ignore[override]
        return self.stat(follow_symlinks=False)

    def exists(self, *, follow_symlinks=True) -> bool:
        return self.fs.exists(self.path)

    def is_dir(self) -> bool:
        return self.fs.isdir(self.path)

    def is_file(self) -> bool:
        return self.fs.isfile(self.path)

    def is_mount(self) -> bool:
        return False

    def is_symlink(self) -> bool:
        try:
            info = self.fs.info(self.path)
            if "islink" in info:
                return bool(info["islink"])
        except FileNotFoundError:
            return False
        return False

    def is_junction(self) -> bool:
        return False

    def is_block_device(self) -> bool:
        return False

    def is_char_device(self) -> bool:
        return False

    def is_fifo(self) -> bool:
        return False

    def is_socket(self) -> bool:
        return False

    def samefile(self, other_path) -> bool:
        st = self.stat()
        if isinstance(other_path, UPath):
            other_st = other_path.stat()
        else:
            other_st = self.with_segments(other_path).stat()
        return st == other_st

    @overload  # type: ignore[override]
    def open(
        self,
        mode: Literal["r", "w", "a"] = "r",
        buffering: int = ...,
        encoding: str = ...,
        errors: str = ...,
        newline: str = ...,
        **fsspec_kwargs: Any,
    ) -> TextIO: ...

    @overload
    def open(  # type: ignore[override]
        self,
        mode: Literal["rb", "wb", "ab"],
        buffering: int = ...,
        encoding: str = ...,
        errors: str = ...,
        newline: str = ...,
        **fsspec_kwargs: Any,
    ) -> BinaryIO: ...

    def open(
        self,
        mode: str = "r",
        *args: Any,
        **fsspec_kwargs: Any,
    ) -> IO[Any]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.

        Parameters
        ----------
        mode:
            Opening mode. Default is 'r'.
        buffering:
            Default is the block size of the underlying fsspec filesystem.
        encoding:
            Encoding is only used in text mode. Default is None.
        errors:
            Error handling for encoding. Only used in text mode. Default is None.
        newline:
            Newline handling. Only used in text mode. Default is None.
        **fsspec_kwargs:
            Additional options for the fsspec filesystem.
        """
        # match the signature of pathlib.Path.open()
        for key, value in zip(["buffering", "encoding", "errors", "newline"], args):
            if key in fsspec_kwargs:
                raise TypeError(
                    f"{type(self).__name__}.open() got multiple values for '{key}'"
                )
            fsspec_kwargs[key] = value
        # translate pathlib buffering to fs block_size
        if "buffering" in fsspec_kwargs:
            fsspec_kwargs.setdefault("block_size", fsspec_kwargs.pop("buffering"))
        return self.fs.open(self.path, mode=mode, **fsspec_kwargs)

    def iterdir(self) -> Generator[UPath, None, None]:
        for name in self.fs.listdir(self.path):
            # fsspec returns dictionaries
            if isinstance(name, dict):
                name = name.get("name")
            if name in {".", ".."}:
                # Yielding a path object for these makes little sense
                continue
            # only want the path name with iterdir
            _, _, name = str_remove_suffix(name, "/").rpartition(self._flavour.sep)
            yield self.with_segments(*self.parts, name)

    def _scandir(self):
        raise NotImplementedError  # todo

    def _make_child_relpath(self, name):
        path = super()._make_child_relpath(name)
        del path._str  # fix _str = str(self) assignment
        return path

    def glob(
        self, pattern: str, *, case_sensitive=None
    ) -> Generator[UPath, None, None]:
        path_pattern = self.joinpath(pattern).path
        sep = self._flavour.sep
        base = self.fs._strip_protocol(self.path)
        for name in self.fs.glob(path_pattern):
            name = str_remove_prefix(str_remove_prefix(name, base), sep)
            yield self.joinpath(name)

    def rglob(
        self, pattern: str, *, case_sensitive=None
    ) -> Generator[UPath, None, None]:
        if _FSSPEC_HAS_WORKING_GLOB is None:
            _check_fsspec_has_working_glob()

        if _FSSPEC_HAS_WORKING_GLOB:
            r_path_pattern = self.joinpath("**", pattern).path
            sep = self._flavour.sep
            base = self.fs._strip_protocol(self.path)
            for name in self.fs.glob(r_path_pattern):
                name = str_remove_prefix(str_remove_prefix(name, base), sep)
                yield self.joinpath(name)

        else:
            path_pattern = self.joinpath(pattern).path
            r_path_pattern = self.joinpath("**", pattern).path
            sep = self._flavour.sep
            base = self.fs._strip_protocol(self.path)
            seen = set()
            for p in (path_pattern, r_path_pattern):
                for name in self.fs.glob(p):
                    name = str_remove_prefix(str_remove_prefix(name, base), sep)
                    if name in seen:
                        continue
                    else:
                        seen.add(name)
                        yield self.joinpath(name)

    @classmethod
    def cwd(cls) -> UPath:
        if cls is UPath:
            return get_upath_class("").cwd()  # type: ignore[union-attr]
        else:
            raise NotImplementedError

    @classmethod
    def home(cls) -> UPath:
        if cls is UPath:
            return get_upath_class("").home()  # type: ignore[union-attr]
        else:
            raise NotImplementedError

    def absolute(self) -> Self:
        return self

    def is_absolute(self) -> bool:
        return self._flavour.isabs(str(self))

    def resolve(self, strict: bool = False) -> Self:
        _parts = self.parts

        # Do not attempt to normalize path if no parts are dots
        if ".." not in _parts and "." not in _parts:
            return self

        resolved: list[str] = []
        resolvable_parts = _parts[1:]
        for part in resolvable_parts:
            if part == "..":
                if resolved:
                    resolved.pop()
            elif part != ".":
                resolved.append(part)

        return self.with_segments(*_parts[:1], *resolved)

    def owner(self) -> str:
        raise NotImplementedError

    def group(self) -> str:
        raise NotImplementedError

    def readlink(self) -> Self:
        raise NotImplementedError

    def touch(self, mode=0o666, exist_ok=True) -> None:
        exists = self.fs.exists(self.path)
        if exists and not exist_ok:
            raise FileExistsError(str(self))
        if not exists:
            self.fs.touch(self.path, truncate=True)
        else:
            try:
                self.fs.touch(self.path, truncate=False)
            except (NotImplementedError, ValueError):
                pass  # unsupported by filesystem

    def mkdir(self, mode=0o777, parents=False, exist_ok=False) -> None:
        if parents and not exist_ok and self.exists():
            raise FileExistsError(str(self))
        try:
            self.fs.mkdir(
                self.path,
                create_parents=parents,
                mode=mode,
            )
        except FileExistsError:
            if not exist_ok:
                raise FileExistsError(str(self))
            if not self.is_dir():
                raise FileExistsError(str(self))

    def chmod(self, mode: int, *, follow_symlinks: bool = True) -> None:
        raise NotImplementedError

    def lchmod(self, mode: int) -> None:
        raise NotImplementedError

    def unlink(self, missing_ok: bool = False) -> None:
        if not self.exists():
            if not missing_ok:
                raise FileNotFoundError(str(self))
            return
        self.fs.rm(self.path, recursive=False)

    def rmdir(self, recursive: bool = True) -> None:  # fixme: non-standard
        if not self.is_dir():
            raise NotADirectoryError(str(self))
        if not recursive and next(self.iterdir()):  # type: ignore[arg-type]
            raise OSError(f"Not recursive and directory not empty: {self}")
        self.fs.rm(self.path, recursive=recursive)

    def rename(
        self,
        target: str | os.PathLike[str] | UPath,
        *,  # note: non-standard compared to pathlib
        recursive: bool = _unset,
        maxdepth: int | None = _unset,
        **kwargs: Any,
    ) -> Self:
        if isinstance(target, str) and self.storage_options:
            target = UPath(target, **self.storage_options)
        target_protocol = get_upath_protocol(target)
        if target_protocol:
            if target_protocol != self.protocol:
                raise ValueError(
                    f"expected protocol {self.protocol!r}, got: {target_protocol!r}"
                )
            if not isinstance(target, UPath):
                target_ = UPath(target, **self.storage_options)
            else:
                target_ = target
            # avoid calling .resolve for subclasses of UPath
            if ".." in target_.parts or "." in target_.parts:
                target_ = target_.resolve()
        else:
            parent = self.parent
            # avoid calling .resolve for subclasses of UPath
            if ".." in parent.parts or "." in parent.parts:
                parent = parent.resolve()
            target_ = parent.joinpath(os.path.normpath(target))
        assert isinstance(target_, type(self)), "identical protocols enforced above"
        if recursive is not _unset:
            kwargs["recursive"] = recursive
        if maxdepth is not _unset:
            kwargs["maxdepth"] = maxdepth
        self.fs.mv(
            self.path,
            target_.path,
            **kwargs,
        )
        return target_

    def replace(self, target: str | os.PathLike[str] | UPath) -> UPath:
        raise NotImplementedError  # todo

    def symlink_to(  # type: ignore[override]
        self,
        target: str | os.PathLike[str] | UPath,
        target_is_directory: bool = False,
    ) -> None:
        raise NotImplementedError

    def hardlink_to(  # type: ignore[override]
        self,
        target: str | os.PathLike[str] | UPath,
    ) -> None:
        raise NotImplementedError

    def expanduser(self) -> Self:
        return self
