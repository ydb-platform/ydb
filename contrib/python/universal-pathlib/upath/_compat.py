from __future__ import annotations

import ntpath
import os
import posixpath
import sys
import warnings
from collections.abc import Sequence
from functools import wraps
from pathlib import Path
from pathlib import PurePath
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import TypeVar
from urllib.parse import SplitResult

from fsspec import get_filesystem_class

if TYPE_CHECKING:
    from upath import UPath

__all__ = [
    "PathlibPathShim",
    "str_remove_prefix",
    "str_remove_suffix",
    "FSSpecAccessorShim",
    "deprecated",
]


if sys.version_info >= (3, 12):  # noqa: C901

    class PathlibPathShim:
        """no need to shim pathlib.Path in Python 3.12+"""

        __slots__ = ()
        __missing_py312_slots__ = ()

        def __init__(self, *args):
            super().__init__(*args)

else:

    def _get_missing_py312_pathlib_slots():
        """Return a tuple of slots that are present in Python 3.12's
        pathlib.Path but not in the current version of pathlib.Path
        """
        py312_slots = (
            "_raw_paths",
            "_drv",
            "_root",
            "_tail_cached",
            "_str",
            "_str_normcase_cached",
            "_parts_normcase_cached",
            "_lines_cached",
            "_hash",
        )
        current_slots = [
            slot for cls in Path.__mro__ for slot in getattr(cls, "__slots__", [])
        ]
        return tuple([slot for slot in py312_slots if slot not in current_slots])

    class PathlibPathShim:
        """A compatibility shim for python < 3.12

        Basically vendoring the functionality of pathlib.Path from Python 3.12
        that's not overwritten in upath.core.UPath

        """

        __slots__ = ()
        __missing_py312_slots__ = _get_missing_py312_pathlib_slots()

        def __init__(self, *args):
            paths = []
            for arg in args:
                if isinstance(arg, PurePath) and hasattr(arg, "_raw_paths"):
                    if arg._flavour is ntpath and self._flavour is posixpath:
                        # GH-103631: Convert separators for backwards compatibility.
                        paths.extend(path.replace("\\", "/") for path in arg._raw_paths)
                    else:
                        paths.extend(arg._raw_paths)
                else:
                    try:
                        path = os.fspath(arg)
                    except TypeError:
                        path = arg
                    if not isinstance(path, str):
                        raise TypeError(
                            "argument should be a str or an os.PathLike "
                            "object where __fspath__ returns a str, "
                            f"not {type(path).__name__!r}"
                        )
                    paths.append(path)
            self._raw_paths = paths

        @classmethod
        def _parse_path(cls, path):
            if not path:
                return "", "", []
            sep = cls._flavour.sep
            altsep = cls._flavour.altsep
            if altsep:
                path = path.replace(altsep, sep)
            drv, root, rel = cls._flavour.splitroot(path)
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

        def _load_parts(self):
            paths = self._raw_paths
            if len(paths) == 0:
                path = ""
            elif len(paths) == 1:
                path = paths[0]
            else:
                path = self._flavour.join(*paths)
            drv, root, tail = self._parse_path(path)
            self._drv = drv
            self._root = root
            self._tail_cached = tail

        def _from_parsed_parts(self, drv, root, tail):
            path_str = self._format_parsed_parts(drv, root, tail)
            path = self.with_segments(path_str)
            path._str = path_str or "."
            path._drv = drv
            path._root = root
            path._tail_cached = tail
            return path

        @classmethod
        def _format_parsed_parts(cls, drv, root, tail):
            if drv or root:
                return drv + root + cls._flavour.sep.join(tail)
            elif tail and cls._flavour.splitdrive(tail[0])[0]:
                tail = ["."] + tail
            return cls._flavour.sep.join(tail)

        def __str__(self):
            try:
                return self._str
            except AttributeError:
                self._str = (
                    self._format_parsed_parts(self.drive, self.root, self._tail) or "."
                )
                return self._str

        @property
        def drive(self):
            try:
                return self._drv
            except AttributeError:
                self._load_parts()
                return self._drv

        @property
        def root(self):
            try:
                return self._root
            except AttributeError:
                self._load_parts()
                return self._root

        @property
        def _tail(self):
            try:
                return self._tail_cached
            except AttributeError:
                self._load_parts()
                return self._tail_cached

        @property
        def anchor(self):
            anchor = self.drive + self.root
            return anchor

        @property
        def name(self):
            tail = self._tail
            if not tail:
                return ""
            return tail[-1]

        @property
        def suffix(self):
            name = self.name
            i = name.rfind(".")
            if 0 < i < len(name) - 1:
                return name[i:]
            else:
                return ""

        @property
        def suffixes(self):
            name = self.name
            if name.endswith("."):
                return []
            name = name.lstrip(".")
            return ["." + suffix for suffix in name.split(".")[1:]]

        @property
        def stem(self):
            name = self.name
            i = name.rfind(".")
            if 0 < i < len(name) - 1:
                return name[:i]
            else:
                return name

        def with_name(self, name):
            if not self.name:
                raise ValueError(f"{self!r} has an empty name")
            f = self._flavour
            if (
                not name
                or f.sep in name
                or (f.altsep and f.altsep in name)
                or name == "."
            ):
                raise ValueError("Invalid name %r" % (name))
            return self._from_parsed_parts(
                self.drive, self.root, self._tail[:-1] + [name]
            )

        def with_stem(self, stem):
            return self.with_name(stem + self.suffix)

        def with_suffix(self, suffix):
            f = self._flavour
            if f.sep in suffix or f.altsep and f.altsep in suffix:
                raise ValueError(f"Invalid suffix {suffix!r}")
            if suffix and not suffix.startswith(".") or suffix == ".":
                raise ValueError("Invalid suffix %r" % (suffix))
            name = self.name
            if not name:
                raise ValueError(f"{self!r} has an empty name")
            old_suffix = self.suffix
            if not old_suffix:
                name = name + suffix
            else:
                name = name[: -len(old_suffix)] + suffix
            return self._from_parsed_parts(
                self.drive, self.root, self._tail[:-1] + [name]
            )

        def relative_to(self, other, /, *_deprecated, walk_up=False):
            if _deprecated:
                msg = (
                    "support for supplying more than one positional argument "
                    "to pathlib.PurePath.relative_to() is deprecated and "
                    "scheduled for removal in Python 3.14"
                )
                warnings.warn(
                    f"pathlib.PurePath.relative_to(*args) {msg}",
                    DeprecationWarning,
                    stacklevel=2,
                )
            other = self.with_segments(other, *_deprecated)
            for step, path in enumerate([other] + list(other.parents)):  # noqa: B007
                if self.is_relative_to(path):
                    break
                elif not walk_up:
                    raise ValueError(
                        f"{str(self)!r} is not in the subpath of {str(other)!r}"
                    )
                elif path.name == "..":
                    raise ValueError(f"'..' segment in {str(other)!r} cannot be walked")
            else:
                raise ValueError(
                    f"{str(self)!r} and {str(other)!r} have different anchors"
                )
            parts = [".."] * step + self._tail[len(path._tail) :]
            return self.with_segments(*parts)

        def is_relative_to(self, other, /, *_deprecated):
            if _deprecated:
                msg = (
                    "support for supplying more than one argument to "
                    "pathlib.PurePath.is_relative_to() is deprecated and "
                    "scheduled for removal in Python 3.14"
                )
                warnings.warn(
                    f"pathlib.PurePath.is_relative_to(*args) {msg}",
                    DeprecationWarning,
                    stacklevel=2,
                )
            other = self.with_segments(other, *_deprecated)
            return other == self or other in self.parents

        @property
        def parts(self):
            if self.drive or self.root:
                return (self.drive + self.root,) + tuple(self._tail)
            else:
                return tuple(self._tail)

        @property
        def parent(self):
            drv = self.drive
            root = self.root
            tail = self._tail
            if not tail:
                return self
            return self._from_parsed_parts(drv, root, tail[:-1])

        @property
        def parents(self):
            return _PathParents(self)

        def _make_child_relpath(self, name):
            path_str = str(self)
            tail = self._tail
            if tail:
                path_str = f"{path_str}{self._flavour.sep}{name}"
            elif path_str != ".":
                path_str = f"{path_str}{name}"
            else:
                path_str = name
            path = self.with_segments(path_str)
            path._str = path_str
            path._drv = self.drive
            path._root = self.root
            path._tail_cached = tail + [name]
            return path

        def lchmod(self, mode):
            """
            Like chmod(), except if the path points to a symlink, the symlink's
            permissions are changed, rather than its target's.
            """
            self.chmod(mode, follow_symlinks=False)

    class _PathParents(Sequence):
        __slots__ = ("_path", "_drv", "_root", "_tail")

        def __init__(self, path):
            self._path = path
            self._drv = path.drive
            self._root = path.root
            self._tail = path._tail

        def __len__(self):
            return len(self._tail)

        def __getitem__(self, idx):
            if isinstance(idx, slice):
                return tuple(self[i] for i in range(*idx.indices(len(self))))

            if idx >= len(self) or idx < -len(self):
                raise IndexError(idx)
            if idx < 0:
                idx += len(self)
            return self._path._from_parsed_parts(
                self._drv, self._root, self._tail[: -idx - 1]
            )

        def __repr__(self):
            return f"<{type(self._path).__name__}.parents>"


if sys.version_info >= (3, 9):
    str_remove_suffix = str.removesuffix
    str_remove_prefix = str.removeprefix

else:

    def str_remove_suffix(s: str, suffix: str) -> str:
        if s.endswith(suffix):
            return s[: -len(suffix)]
        else:
            return s

    def str_remove_prefix(s: str, prefix: str) -> str:
        if s.startswith(prefix):
            return s[len(prefix) :]
        else:
            return s


class FSSpecAccessorShim:
    """this is a compatibility shim and will be removed"""

    def __init__(self, parsed_url: SplitResult | None, **kwargs: Any) -> None:
        if parsed_url and parsed_url.scheme:
            cls = get_filesystem_class(parsed_url.scheme)
            url_kwargs = cls._get_kwargs_from_urls(parsed_url.geturl())
        else:
            cls = get_filesystem_class(None)
            url_kwargs = {}
        url_kwargs.update(kwargs)
        self._fs = cls(**url_kwargs)

    def __init_subclass__(cls, **kwargs):
        warnings.warn(
            "All _FSSpecAccessor subclasses have been deprecated. "
            " Please follow the universal_pathlib==0.2.0 migration guide at"
            " https://github.com/fsspec/universal_pathlib for more"
            " information.",
            DeprecationWarning,
            stacklevel=2,
        )

    @classmethod
    def from_path(cls, path: UPath) -> FSSpecAccessorShim:
        """internal accessor for backwards compatibility"""
        url = path._url._replace(scheme=path.protocol)
        obj = cls(url, **path.storage_options)
        obj.__dict__["_fs"] = path.fs
        return obj

    def _format_path(self, path: UPath) -> str:
        return path.path

    def open(self, path, mode="r", *args, **kwargs):
        return path.fs.open(self._format_path(path), mode, *args, **kwargs)

    def stat(self, path, **kwargs):
        return path.fs.stat(self._format_path(path), **kwargs)

    def listdir(self, path, **kwargs):
        p_fmt = self._format_path(path)
        contents = path.fs.listdir(p_fmt, **kwargs)
        if len(contents) == 0 and not path.fs.isdir(p_fmt):
            raise NotADirectoryError(str(self))
        elif (
            len(contents) == 1
            and contents[0]["name"] == p_fmt
            and contents[0]["type"] == "file"
        ):
            raise NotADirectoryError(str(self))
        return contents

    def glob(self, _path, path_pattern, **kwargs):
        return _path.fs.glob(self._format_path(path_pattern), **kwargs)

    def exists(self, path, **kwargs):
        return path.fs.exists(self._format_path(path), **kwargs)

    def info(self, path, **kwargs):
        return path.fs.info(self._format_path(path), **kwargs)

    def rm(self, path, recursive, **kwargs):
        return path.fs.rm(self._format_path(path), recursive=recursive, **kwargs)

    def mkdir(self, path, create_parents=True, **kwargs):
        return path.fs.mkdir(
            self._format_path(path), create_parents=create_parents, **kwargs
        )

    def makedirs(self, path, exist_ok=False, **kwargs):
        return path.fs.makedirs(self._format_path(path), exist_ok=exist_ok, **kwargs)

    def touch(self, path, **kwargs):
        return path.fs.touch(self._format_path(path), **kwargs)

    def mv(self, path, target, recursive=False, maxdepth=None, **kwargs):
        if hasattr(target, "_accessor"):
            target = target._accessor._format_path(target)
        return path.fs.mv(
            self._format_path(path),
            target,
            recursive=recursive,
            maxdepth=maxdepth,
            **kwargs,
        )


RT = TypeVar("RT")
F = Callable[..., RT]


def deprecated(*, python_version: tuple[int, ...]) -> Callable[[F], F]:
    """marks function as deprecated"""
    pyver_str = ".".join(map(str, python_version))

    def deprecated_decorator(func: F) -> F:
        if sys.version_info >= python_version:

            @wraps(func)
            def wrapper(*args, **kwargs):
                warnings.warn(
                    f"{func.__name__} is deprecated on py>={pyver_str}",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return func(*args, **kwargs)

            return wrapper

        else:
            return func

    return deprecated_decorator


class method_and_classmethod:
    """Allow a method to be used as both a method and a classmethod"""

    def __init__(self, method):
        self.method = method

    def __get__(self, instance, owner):
        if instance is None:
            return self.method.__get__(owner)
        return self.method.__get__(instance)
