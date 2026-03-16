import abc
from collections import defaultdict
import collections.abc
from contextlib import contextmanager
from io import BufferedRandom, BufferedReader, BufferedWriter, FileIO, TextIOWrapper
import os
from pathlib import (  # type: ignore
    Path,
    PosixPath,
    PurePosixPath,
    WindowsPath,
)

import shutil
import sys
from types import MethodType
from typing import (
    BinaryIO,
    Literal,
    overload,
    Any,
    Callable,
    Container,
    Iterable,
    IO,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import urlparse
from warnings import warn

if TYPE_CHECKING:
    from _typeshed import (
        OpenBinaryMode,
        OpenBinaryModeReading,
        OpenBinaryModeUpdating,
        OpenBinaryModeWriting,
        OpenTextMode,
    )

if sys.version_info >= (3, 10):
    from typing import TypeGuard
else:
    from typing_extensions import TypeGuard

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


if sys.version_info < (3, 12):
    from pathlib import _posix_flavour  # type: ignore[attr-defined] # noqa: F811
    from pathlib import _make_selector as _make_selector_pathlib  # type: ignore[attr-defined] # noqa: F811
    from pathlib import _PathParents  # type: ignore[attr-defined]

    def _make_selector(pattern_parts, _flavour, case_sensitive=True):  # noqa: F811
        return _make_selector_pathlib(tuple(pattern_parts), _flavour)

elif sys.version_info[:2] == (3, 12):
    from pathlib import _PathParents  # type: ignore[attr-defined]
    from pathlib import posixpath as _posix_flavour  # type: ignore[attr-defined]
    from pathlib import _make_selector  # type: ignore[attr-defined]
elif sys.version_info[:2] == (3, 13):
    from pathlib._local import _PathParents
    import posixpath as _posix_flavour  # type: ignore[attr-defined]   # noqa: F811

    from .legacy.glob import _make_selector  # noqa: F811
elif sys.version_info >= (3, 14):
    from pathlib import _PathParents  # type: ignore[attr-defined]
    import posixpath as _posix_flavour  # type: ignore[attr-defined]
    from .legacy.glob import _make_selector  # noqa: F811


from cloudpathlib.enums import FileCacheMode

from . import anypath
from .exceptions import (
    ClientMismatchError,
    CloudPathFileExistsError,
    CloudPathFileNotFoundError,
    CloudPathIsADirectoryError,
    CloudPathNotADirectoryError,
    CloudPathNotExistsError,
    CloudPathNotImplementedError,
    DirectoryNotEmptyError,
    IncompleteImplementationError,
    InvalidPrefixError,
    MissingDependenciesError,
    NoStatError,
    OverwriteDirtyFileError,
    OverwriteNewerCloudError,
    OverwriteNewerLocalError,
)


if TYPE_CHECKING:
    from .client import Client

from .cloudpath_info import CloudPathInfo


class CloudImplementation:
    name: str
    dependencies_loaded: bool = True
    _client_class: Type["Client"]
    _path_class: Type["CloudPath"]

    def validate_completeness(self) -> None:
        expected = ["client_class", "path_class"]
        missing = [cls for cls in expected if getattr(self, f"_{cls}") is None]
        if missing:
            raise IncompleteImplementationError(
                f"Implementation is missing registered components: {missing}"
            )
        if not self.dependencies_loaded:
            raise MissingDependenciesError(
                f"Missing dependencies for {self._client_class.__name__}. You can install them "
                f"with 'pip install cloudpathlib[{self.name}]'."
            )

    @property
    def client_class(self) -> Type["Client"]:
        self.validate_completeness()
        return self._client_class

    @property
    def path_class(self) -> Type["CloudPath"]:
        self.validate_completeness()
        return self._path_class


implementation_registry: Dict[str, CloudImplementation] = defaultdict(CloudImplementation)


T = TypeVar("T")
CloudPathT = TypeVar("CloudPathT", bound="CloudPath")


def register_path_class(key: str) -> Callable[[Type[CloudPathT]], Type[CloudPathT]]:
    def decorator(cls: Type[CloudPathT]) -> Type[CloudPathT]:
        if not issubclass(cls, CloudPath):
            raise TypeError("Only subclasses of CloudPath can be registered.")
        implementation_registry[key]._path_class = cls
        cls._cloud_meta = implementation_registry[key]
        return cls

    return decorator


class CloudPathMeta(abc.ABCMeta):
    @overload
    def __call__(
        cls: Type[T], cloud_path: CloudPathT, *args: Any, **kwargs: Any
    ) -> CloudPathT: ...

    @overload
    def __call__(
        cls: Type[T], cloud_path: Union[str, "CloudPath"], *args: Any, **kwargs: Any
    ) -> T: ...

    def __call__(
        cls: Type[T], cloud_path: Union[str, CloudPathT], *args: Any, **kwargs: Any
    ) -> Union[T, "CloudPath", CloudPathT]:
        # cls is a class that is the instance of this metaclass, e.g., CloudPath
        if not issubclass(cls, CloudPath):
            raise TypeError(
                f"Only subclasses of {CloudPath.__name__} can be instantiated from its meta class."
            )

        # Dispatch to subclass if base CloudPath
        if cls is CloudPath:
            for implementation in implementation_registry.values():
                path_class = implementation._path_class
                if path_class is not None and path_class.is_valid_cloudpath(
                    cloud_path, raise_on_error=False
                ):
                    # Instantiate path_class instance
                    new_obj = object.__new__(path_class)
                    path_class.__init__(new_obj, cloud_path, *args, **kwargs)  # type: ignore[type-var]
                    return new_obj
            valid_prefixes = [
                impl._path_class.cloud_prefix
                for impl in implementation_registry.values()
                if impl._path_class is not None
            ]
            raise InvalidPrefixError(
                f"Path {cloud_path} does not begin with a known prefix {valid_prefixes}."
            )

        new_obj = object.__new__(cls)
        cls.__init__(new_obj, cloud_path, *args, **kwargs)  # type: ignore[type-var]
        return new_obj

    def __init__(cls, name: str, bases: Tuple[type, ...], dic: Dict[str, Any]) -> None:
        # Copy docstring from pathlib.Path
        for attr in dir(cls):
            if (
                not attr.startswith("_")
                and hasattr(Path, attr)
                and getattr(getattr(Path, attr), "__doc__", None)
            ):
                docstring = getattr(Path, attr).__doc__ + " _(Docstring copied from pathlib.Path)_"

                if isinstance(getattr(cls, attr), (MethodType)):
                    getattr(cls, attr).__func__.__doc__ = docstring
                else:
                    getattr(cls, attr).__doc__ = docstring

                if isinstance(getattr(cls, attr), property):
                    # Properties have __doc__ duplicated under fget, and at least some parsers
                    # read it from there.
                    getattr(cls, attr).fget.__doc__ = docstring


# Abstract base class
class CloudPath(metaclass=CloudPathMeta):
    """Base class for cloud storage file URIs, in the style of the Python standard library's
    [`pathlib` module](https://docs.python.org/3/library/pathlib.html). Instances represent a path
    in cloud storage with filesystem path semantics, and convenient methods allow for basic
    operations like joining, reading, writing, iterating over contents, etc. `CloudPath` almost
    entirely mimics the [`pathlib.Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path)
    interface, so most familiar properties and methods should be available and behave in the
    expected way.

    Analogous to the way `pathlib.Path` works, instantiating `CloudPath` will instead create an
    instance of an appropriate subclass that implements a particular cloud storage service, such as
    [`S3Path`](../s3path). This dispatching behavior is based on the URI scheme part of a cloud
    storage URI (e.g., `"s3://"`).
    """

    _cloud_meta: CloudImplementation
    cloud_prefix: str

    def __init__(
        self,
        cloud_path: Union[str, Self, "CloudPath"],
        *parts: str,
        client: Optional["Client"] = None,
    ) -> None:
        # handle if local file gets opened. must be set at the top of the method in case any code
        # below raises an exception, this prevents __del__ from raising an AttributeError
        self._handle: Optional[IO] = None
        self._client: Optional["Client"] = None

        if parts:
            # ensure first part ends in "/"; (sometimes it is just prefix, sometimes a longer path)
            if not str(cloud_path).endswith("/"):
                cloud_path = str(cloud_path) + "/"

            cloud_path = str(cloud_path) + "/".join(p.strip("/") for p in parts)

        self.is_valid_cloudpath(cloud_path, raise_on_error=True)
        self._cloud_meta.validate_completeness()

        # versions of the raw string that provide useful methods
        self._str = str(cloud_path)
        self._url = urlparse(self._str)
        self._path = PurePosixPath(f"/{self._no_prefix}")

        # setup client
        if client is None:
            if isinstance(cloud_path, CloudPath):
                self._client = cloud_path.client
        else:
            self._client = client

        if client is not None and not isinstance(client, self._cloud_meta.client_class):
            raise ClientMismatchError(
                f"Client of type [{client.__class__}] is not valid for cloud path of type "
                f"[{self.__class__}]; must be instance of [{self._cloud_meta.client_class}], or "
                f"None to use default client for this cloud path class."
            )

        # track if local has been written to, if so it may need to be uploaded
        self._dirty = False

    @property
    def client(self):
        if getattr(self, "_client", None) is None:
            self._client = self._cloud_meta.client_class.get_default_client()

        return self._client

    def __del__(self) -> None:
        # make sure that file handle to local path is closed
        if self._handle is not None and self._local.exists():
            self._handle.close()

        # ensure file removed from cache when cloudpath object deleted
        client = getattr(self, "_client", None)
        if getattr(client, "file_cache_mode", None) == FileCacheMode.cloudpath_object:
            self.clear_cache()

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()

        # don't pickle client
        if "_client" in state:
            del state["_client"]

        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)

    @property
    def _no_prefix(self) -> str:
        return self._str[len(self.anchor) :]

    @property
    def _no_prefix_no_drive(self) -> str:
        return self._str[len(self.anchor) + len(self.drive) :]

    @overload
    @classmethod
    def is_valid_cloudpath(
        cls, path: "CloudPath", raise_on_error: bool = ...
    ) -> TypeGuard[Self]: ...

    @overload
    @classmethod
    def is_valid_cloudpath(cls, path: str, raise_on_error: bool = ...) -> bool: ...

    @classmethod
    def is_valid_cloudpath(
        cls, path: Union[str, "CloudPath"], raise_on_error: bool = False
    ) -> Union[bool, TypeGuard[Self]]:
        valid = str(path).lower().startswith(cls.cloud_prefix.lower())

        if raise_on_error and not valid:
            raise InvalidPrefixError(
                f"'{path}' is not a valid path since it does not start with '{cls.cloud_prefix}'"
            )

        return valid

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self}')"

    def __str__(self) -> str:
        return self._str

    def __hash__(self) -> int:
        return hash((type(self).__name__, str(self)))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and str(self) == str(other)

    def __fspath__(self) -> str:
        if self.is_file():
            self._refresh_cache()
        return str(self._local)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.parts < other.parts

    def __le__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.parts <= other.parts

    def __gt__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.parts > other.parts

    def __ge__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.parts >= other.parts

    # ====================== NOT IMPLEMENTED ======================
    # as_posix - no cloud equivalent; not needed since we assume url separator
    # chmod - permission changing should be explicitly done per client with methods
    #           that make sense for the client permission options
    # cwd - no cloud equivalent
    # expanduser - no cloud equivalent
    # group - should be implemented with client-specific permissions
    # home - no cloud equivalent
    # is_block_device - no cloud equivalent
    # is_char_device - no cloud equivalent
    # is_fifo - no cloud equivalent
    # is_mount - no cloud equivalent
    # is_reserved - no cloud equivalent
    # is_socket - no cloud equivalent
    # is_symlink - no cloud equivalent
    # lchmod - no cloud equivalent
    # lstat - no cloud equivalent
    # owner - no cloud equivalent
    # readlink - no cloud equivalent
    # root - drive already has the bucket and anchor/prefix has the scheme, so nothing to store here
    # symlink_to - no cloud equivalent
    # link_to - no cloud equivalent
    # hardlink_to - no cloud equivalent

    # ====================== REQUIRED, NOT GENERIC ======================
    # Methods that must be implemented, but have no generic application
    @property
    @abc.abstractmethod
    def drive(self) -> str:
        """For example "bucket" on S3 or "container" on Azure; needs to be defined for each class"""
        pass

    @abc.abstractmethod
    def mkdir(
        self, parents: bool = False, exist_ok: bool = False, mode: Optional[Any] = None
    ) -> None:
        """Should be implemented using the client API without requiring a dir is downloaded"""
        pass

    @abc.abstractmethod
    def touch(self, exist_ok: bool = True, mode: Optional[Any] = None) -> None:
        """Should be implemented using the client API to create and update modified time"""
        pass

    def as_url(self, presign: bool = False, expire_seconds: int = 60 * 60) -> str:
        if presign:
            url = self.client._generate_presigned_url(self, expire_seconds=expire_seconds)
        else:
            url = self.client._get_public_url(self)
        return url

    # ====================== IMPLEMENTED FROM SCRATCH ======================
    # Methods with their own implementations that work generically
    def __rtruediv__(self, other: Any) -> None:
        raise ValueError(
            "Cannot change a cloud path's root since all paths are absolute; create a new path instead."
        )

    @property
    def anchor(self) -> str:
        return self.cloud_prefix

    def as_uri(self) -> str:
        return str(self)

    def exists(self, follow_symlinks=True) -> bool:
        return self.client._exists(self)

    def is_dir(self, follow_symlinks=True) -> bool:
        return self.client._is_file_or_dir(self) == "dir"

    def is_file(self, follow_symlinks=True) -> bool:
        return self.client._is_file_or_dir(self) == "file"

    @property
    def fspath(self) -> str:
        return self.__fspath__()

    @classmethod
    def from_uri(cls, uri: str) -> Self:
        return cls(uri)

    def _glob_checks(self, pattern: Union[str, os.PathLike]) -> str:
        if isinstance(pattern, os.PathLike):
            if isinstance(pattern, CloudPath):
                str_pattern = str(pattern.relative_to(self))
            else:
                str_pattern = os.fspath(pattern)
        else:
            str_pattern = str(pattern)

        if ".." in str_pattern:
            raise CloudPathNotImplementedError(
                "Relative paths with '..' not supported in glob patterns."
            )

        if str_pattern.startswith(self.cloud_prefix) or str_pattern.startswith("/"):
            raise CloudPathNotImplementedError("Non-relative patterns are unsupported")

        if self.drive == "":
            raise CloudPathNotImplementedError(
                ".glob is only supported within a bucket or container; you can use `.iterdir` to list buckets; for example, CloudPath('s3://').iterdir()"
            )

        return str_pattern

    def _build_subtree(self, recursive):
        # build a tree structure for all files out of default dicts
        Tree: Callable = lambda: defaultdict(Tree)

        def _build_tree(trunk, branch, nodes, is_dir):
            """Utility to build a tree from nested defaultdicts with a generator
            of nodes (parts) of a path."""
            next_branch = next(nodes, None)

            if next_branch is None:
                trunk[branch] = Tree() if is_dir else None  # leaf node

            else:
                _build_tree(trunk[branch], next_branch, nodes, is_dir)

        file_tree = Tree()

        for f, is_dir in self.client._list_dir(self, recursive=recursive):
            parts = str(f.relative_to(self)).split("/")

            # skip self
            if len(parts) == 1 and parts[0] == ".":
                continue

            nodes = (p for p in parts)
            _build_tree(file_tree, next(nodes, None), nodes, is_dir)

        return dict(file_tree)  # freeze as normal dict before passing in

    def _glob(self, selector, recursive: bool) -> Generator[Self, None, None]:
        file_tree = self._build_subtree(recursive)

        root = _CloudPathSelectable(
            self.name,
            [],  # nothing above self will be returned, so initial parents is empty
            file_tree,
        )

        for p in selector.select_from(root):
            # select_from returns self.name/... so strip before joining
            yield (self / str(p)[len(self.name) + 1 :])

    def glob(
        self,
        pattern: Union[str, os.PathLike],
        case_sensitive: Optional[bool] = None,
        recurse_symlinks: bool = True,
    ) -> Generator[Self, None, None]:
        pattern = self._glob_checks(pattern)

        pattern_parts = PurePosixPath(pattern).parts
        selector = _make_selector(
            tuple(pattern_parts), _posix_flavour, case_sensitive=case_sensitive
        )

        yield from self._glob(
            selector,
            "/" in pattern
            or "**"
            in pattern,  # recursive listing needed if explicit ** or any sub folder in pattern
        )

    def rglob(
        self,
        pattern: Union[str, os.PathLike],
        case_sensitive: Optional[bool] = None,
        recurse_symlinks: bool = True,
    ) -> Generator[Self, None, None]:
        pattern = self._glob_checks(pattern)

        pattern_parts = PurePosixPath(pattern).parts
        selector = _make_selector(
            ("**",) + tuple(pattern_parts), _posix_flavour, case_sensitive=case_sensitive
        )

        yield from self._glob(selector, True)

    def iterdir(self) -> Generator[Self, None, None]:
        for f, _ in self.client._list_dir(self, recursive=False):
            if f != self:  # iterdir does not include itself in pathlib
                yield f

    @staticmethod
    def _walk_results_from_tree(root, tree, top_down=True):
        """Utility to yield tuples in the form expected by `.walk` from the file
        tree constructed by `_build_substree`.
        """
        dirs = []
        files = []
        for item, branch in tree.items():
            files.append(item) if branch is None else dirs.append(item)

        if top_down:
            yield root, dirs, files

        for dir in dirs:
            yield from CloudPath._walk_results_from_tree(root / dir, tree[dir], top_down=top_down)

        if not top_down:
            yield root, dirs, files

    def walk(
        self,
        top_down: bool = True,
        on_error: Optional[Callable] = None,
        follow_symlinks: bool = False,
    ) -> Generator[Tuple[Self, List[str], List[str]], None, None]:
        try:
            file_tree = self._build_subtree(recursive=True)  # walking is always recursive
            yield from self._walk_results_from_tree(self, file_tree, top_down=top_down)

        except Exception as e:
            if on_error is not None:
                on_error(e)
            else:
                raise

    @overload
    def open(
        self,
        mode: "OpenTextMode" = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        force_overwrite_from_cloud: Optional[bool] = None,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> "TextIOWrapper": ...

    @overload
    def open(
        self,
        mode: "OpenBinaryMode",
        buffering: Literal[0],
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: Optional[bool] = None,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> "FileIO": ...

    @overload
    def open(
        self,
        mode: "OpenBinaryModeUpdating",
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: Optional[bool] = None,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> "BufferedRandom": ...

    @overload
    def open(
        self,
        mode: "OpenBinaryModeWriting",
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: Optional[bool] = None,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> "BufferedWriter": ...

    @overload
    def open(
        self,
        mode: "OpenBinaryModeReading",
        buffering: Literal[-1, 1] = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: Optional[bool] = None,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> "BufferedReader": ...

    @overload
    def open(
        self,
        mode: "OpenBinaryMode",
        buffering: int = -1,
        encoding: None = None,
        errors: None = None,
        newline: None = None,
        force_overwrite_from_cloud: Optional[bool] = None,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> "BinaryIO": ...

    @overload
    def open(
        self,
        mode: str,
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        force_overwrite_from_cloud: Optional[bool] = None,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> "IO[Any]": ...

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        force_overwrite_from_cloud: Optional[bool] = None,  # extra kwarg not in pathlib
        force_overwrite_to_cloud: Optional[bool] = None,  # extra kwarg not in pathlib
    ) -> "IO[Any]":
        # if trying to call open on a directory that exists
        exists_on_cloud = self.exists()

        if exists_on_cloud and not self.is_file():
            raise CloudPathIsADirectoryError(
                f"Cannot open directory, only files. Tried to open ({self})"
            )

        if not exists_on_cloud and any(m in mode for m in ("r", "a")):
            raise CloudPathFileNotFoundError(
                f"File opened for read or append, but it does not exist on cloud: {self}"
            )

        if mode == "x" and self.exists():
            raise CloudPathFileExistsError(f"Cannot open existing file ({self}) for creation.")

        # TODO: consider streaming from client rather than DLing entire file to cache
        self._refresh_cache(force_overwrite_from_cloud=force_overwrite_from_cloud)

        # create any directories that may be needed if the file is new
        if not self._local.exists():
            self._local.parent.mkdir(parents=True, exist_ok=True)
            original_mtime = 0.0
        else:
            original_mtime = self._local.stat().st_mtime

        buffer = self._local.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

        # write modes need special on closing the buffer
        if any(m in mode for m in ("w", "+", "x", "a")):
            # dirty, handle, patch close
            wrapped_close = buffer.close

            # since we are pretending this is a cloud file, upload it to the cloud
            # when the buffer is closed
            def _patched_close_upload(*args, **kwargs) -> None:
                wrapped_close(*args, **kwargs)

                # we should be idempotent and not upload again if
                # we already ran our close method patch
                if not self._dirty:
                    return

                # original mtime should match what was in the cloud; because of system clocks or rounding
                # by the cloud provider, the new version in our cache is "older" than the original version;
                # explicitly set the new modified time to be after the original modified time.
                if self._local.stat().st_mtime < original_mtime:
                    new_mtime = original_mtime + 1
                    os.utime(self._local, times=(new_mtime, new_mtime))

                self._upload_local_to_cloud(force_overwrite_to_cloud=force_overwrite_to_cloud)
                self._dirty = False

            buffer.close = _patched_close_upload  # type: ignore

            # keep reference in case we need to close when __del__ is called on this object
            self._handle = buffer

            # opened for write, so mark dirty
            self._dirty = True

        # if we don't want any cache around, remove the cache
        # as soon as the file is closed
        if self.client.file_cache_mode == FileCacheMode.close_file:
            # this may be _patched_close_upload, in which case we need to
            # make sure to call that first so the file gets uploaded
            wrapped_close_for_cache = buffer.close

            def _patched_close_empty_cache(*args, **kwargs):
                wrapped_close_for_cache(*args, **kwargs)

                # remove local file as last step on closing
                self.clear_cache()

            buffer.close = _patched_close_empty_cache  # type: ignore

        return buffer

    def replace(self, target: Self) -> Self:
        if type(self) is not type(target):
            raise TypeError(
                f"The target based to rename must be an instantiated class of type: {type(self)}"
            )

        if self.is_dir():
            raise CloudPathIsADirectoryError(
                f"Path {self} is a directory; rename/replace the files recursively."
            )

        if target == self:
            # Request is to replace/rename this with the same path - nothing to do
            return self

        if target.exists():
            target.unlink()

        self.client._move_file(self, target)
        return target

    def rename(self, target: Self) -> Self:
        # for cloud services replace == rename since we don't just rename,
        # we actually move files
        return self.replace(target)

    def rmdir(self) -> None:
        if self.is_file():
            raise CloudPathNotADirectoryError(
                f"Path {self} is a file; call unlink instead of rmdir."
            )
        try:
            next(self.iterdir())
            raise DirectoryNotEmptyError(
                f"Directory not empty: '{self}'. Use rmtree to delete recursively."
            )
        except StopIteration:
            pass
        self.client._remove(self)

    def samefile(self, other_path: Union[str, os.PathLike]) -> bool:
        # all cloud paths are absolute and the paths are used for hash
        return self == other_path

    def unlink(self, missing_ok: bool = True) -> None:
        # Note: missing_ok defaults to False in pathlib, but changing the default now would be a breaking change.
        if self.is_dir():
            raise CloudPathIsADirectoryError(
                f"Path {self} is a directory; call rmdir instead of unlink."
            )
        self.client._remove(self, missing_ok)

    def write_bytes(self, data: bytes) -> int:
        """Open the file in bytes mode, write to it, and close the file.

        NOTE: vendored from pathlib since we override open
        https://github.com/python/cpython/blob/3.8/Lib/pathlib.py#L1235-L1242
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode="wb") as f:
            return f.write(view)

    def write_text(
        self,
        data: str,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> int:
        """Open the file in text mode, write to it, and close the file.

        NOTE: vendored from pathlib since we override open
        https://github.com/python/cpython/blob/3.10/Lib/pathlib.py#L1146-L1155
        """
        if not isinstance(data, str):
            raise TypeError("data must be str, not %s" % data.__class__.__name__)

        with self.open(mode="w", encoding=encoding, errors=errors, newline=newline) as f:
            return f.write(data)

    def read_bytes(self) -> bytes:
        with self.open(mode="rb") as f:
            return f.read()

    def read_text(
        self,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> str:
        with self.open(mode="r", encoding=encoding, errors=errors, newline=newline) as f:
            return f.read()

    def is_junction(self):
        return False  # only windows paths can be junctions, not cloudpaths

    # ====================== DISPATCHED TO POSIXPATH FOR PURE PATHS ======================
    # Methods that are dispatched to exactly how pathlib.PurePosixPath would calculate it on
    # self._path for pure paths (does not matter if file exists);
    # see the next session for ones that require a real file to exist
    def _dispatch_to_path(self, func: str, *args, **kwargs) -> Any:
        """Some functions we can just dispatch to the pathlib version
        We want to do this explicitly so we don't have to support all
        of pathlib and subclasses can override individually if necessary.
        """
        path_version = self._path.__getattribute__(func)

        # Path functions should be called so the results are calculated
        if callable(path_version):
            path_version = path_version(*args, **kwargs)

        # Paths should always be resolved and then converted to the same client + class as this one
        if isinstance(path_version, PurePosixPath):
            # always resolve since cloud paths must be absolute
            path_version = _resolve(path_version)
            return self._new_cloudpath(path_version)

        # When sequence of PurePosixPath, we want to convert to sequence of CloudPaths
        if (
            isinstance(path_version, collections.abc.Sequence)
            and len(path_version) > 0
            and isinstance(path_version[0], PurePosixPath)
        ):
            sequence_class = (
                type(path_version) if not isinstance(path_version, _PathParents) else tuple
            )
            return sequence_class(  # type: ignore
                self._new_cloudpath(_resolve(p)) for p in path_version if _resolve(p) != p.root
            )

        # when pathlib returns something else, we probably just want that thing
        # cases this should include: str, empty sequence, sequence of str, ...
        else:
            return path_version

    def __truediv__(self, other: Union[str, PurePosixPath]) -> Self:
        if not isinstance(other, (str, PurePosixPath)):
            return NotImplemented

        return self._dispatch_to_path("__truediv__", other)

    def joinpath(self, *pathsegments: Union[str, os.PathLike]) -> Self:
        return self._dispatch_to_path("joinpath", *pathsegments)

    def absolute(self) -> Self:
        return self

    def is_absolute(self) -> bool:
        return True

    def resolve(self, strict: bool = False) -> Self:
        return self

    def relative_to(self, other: Self, walk_up: bool = False) -> PurePosixPath:
        # We don't dispatch regularly since this never returns a cloud path (since it is relative, and cloud paths are
        # absolute)
        if not isinstance(other, CloudPath):
            raise ValueError(f"{self} is a cloud path, but {other} is not")
        if self.anchor != other.anchor:
            raise ValueError(
                f"{self} is a {self.anchor} path, but {other} is a {other.anchor} path"
            )

        kwargs = dict(walk_up=walk_up)

        if sys.version_info < (3, 12):
            kwargs.pop("walk_up")

        return self._path.relative_to(other._path, **kwargs)  # type: ignore[call-arg]

    def is_relative_to(self, other: Self) -> bool:
        try:
            self.relative_to(other)
            return True
        except ValueError:
            return False

    @property
    def name(self) -> str:
        return self._dispatch_to_path("name")

    def full_match(self, pattern: str, case_sensitive: Optional[bool] = None) -> bool:
        if sys.version_info < (3, 13):
            raise NotImplementedError("full_match requires Python 3.13 or higher")

        # strip scheme from start of pattern before testing
        if pattern.startswith(self.anchor + self.drive):
            pattern = pattern[len(self.anchor + self.drive) :]
        elif pattern.startswith(self.anchor):
            # for http paths, keep leading slash
            pattern = pattern[len(self.anchor) - 1 :]

        # remove drive, which is kept on normal dispatch to pathlib
        return PurePosixPath(self._no_prefix_no_drive).full_match(  # type: ignore[attr-defined]
            pattern, case_sensitive=case_sensitive
        )

    def match(self, path_pattern: str, case_sensitive: Optional[bool] = None) -> bool:
        # strip scheme from start of pattern before testing
        if path_pattern.startswith(self.anchor + self.drive + "/"):
            path_pattern = path_pattern[len(self.anchor + self.drive + "/") :]

        kwargs = dict(case_sensitive=case_sensitive)

        if sys.version_info < (3, 12):
            kwargs.pop("case_sensitive")

        return self._dispatch_to_path("match", path_pattern, **kwargs)

    @property
    def parser(self) -> Self:
        if sys.version_info < (3, 13):
            raise NotImplementedError("parser requires Python 3.13 or higher")

        return self._dispatch_to_path("parser")

    @property
    def parent(self) -> Self:
        return self._dispatch_to_path("parent")

    @property
    def parents(self) -> Tuple[Self, ...]:
        return self._dispatch_to_path("parents")

    @property
    def parts(self) -> Tuple[str, ...]:
        parts = self._dispatch_to_path("parts")
        if parts[0] == "/":
            parts = parts[1:]

        return (self.anchor, *parts)

    @property
    def stem(self) -> str:
        return self._dispatch_to_path("stem")

    @property
    def suffix(self) -> str:
        return self._dispatch_to_path("suffix")

    @property
    def suffixes(self) -> List[str]:
        return self._dispatch_to_path("suffixes")

    def with_stem(self, stem: str) -> Self:
        try:
            return self._dispatch_to_path("with_stem", stem)
        except AttributeError:
            # with_stem was only added in python 3.9, so we fallback for compatibility
            return self.with_name(stem + self.suffix)

    def with_name(self, name: str) -> Self:
        return self._dispatch_to_path("with_name", name)

    def with_segments(self, *pathsegments) -> Self:
        """Create a new CloudPath with the same client out of the given segments.
        The first segment will be interpreted as the bucket/container name.
        """
        return self._new_cloudpath("/".join(pathsegments))

    def with_suffix(self, suffix: str) -> Self:
        return self._dispatch_to_path("with_suffix", suffix)

    # ====================== DISPATCHED TO LOCAL CACHE FOR CONCRETE PATHS ======================
    # Items that can be executed on the cached file on the local filesystem
    def _dispatch_to_local_cache_path(self, func: str, *args, **kwargs) -> Any:
        self._refresh_cache()

        path_version = self._local.__getattribute__(func)

        # Path functions should be called so the results are calculated
        if callable(path_version):
            path_version = path_version(*args, **kwargs)

        # Paths should always be resolved and then converted to the same client + class as this one
        if isinstance(path_version, (PosixPath, WindowsPath)):
            # always resolve since cloud paths must be absolute
            path_version = path_version.resolve()
            return self._new_cloudpath(path_version)

        # when pathlib returns a string, etc. we probably just want that thing
        else:
            return path_version

    def stat(self, follow_symlinks: bool = True) -> os.stat_result:
        """Note: for many clients, we may want to override so we don't incur
        network costs since many of these properties are available as
        API calls.
        """
        warn(
            f"stat not implemented as API call for {self.__class__} so file must be downloaded to "
            f"calculate stats; this may take a long time depending on filesize"
        )
        return self._dispatch_to_local_cache_path("stat", follow_symlinks=follow_symlinks)

    def info(self) -> "CloudPathInfo":
        """Return a CloudPathInfo object for this path."""
        return CloudPathInfo(self)

    # ===========  public cloud methods, not in pathlib ===============
    def download_to(self, destination: Union[str, os.PathLike]) -> Path:
        destination = Path(destination)

        if not self.exists():
            raise CloudPathNotExistsError(f"Cannot download because path does not exist: {self}")

        if self.is_file():
            if destination.is_dir():
                destination = destination / self.name
            return self.client._download_file(self, destination)
        else:
            destination.mkdir(exist_ok=True)
            for f in self.iterdir():
                rel = str(self)
                if not rel.endswith("/"):
                    rel = rel + "/"

                rel_dest = str(f)[len(rel) :]
                f.download_to(destination / rel_dest)

            return destination

    def rmtree(self) -> None:
        """Delete an entire directory tree."""
        if self.is_file():
            raise CloudPathNotADirectoryError(
                f"Path {self} is a file; call unlink instead of rmtree."
            )
        self.client._remove(self)

    def upload_from(
        self,
        source: Union[str, os.PathLike],
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Self:
        """Upload a file or directory to the cloud path."""
        source = Path(source)

        if source.is_dir():
            for p in source.iterdir():
                (self / p.name).upload_from(p, force_overwrite_to_cloud=force_overwrite_to_cloud)

            return self

        else:
            if self.exists() and self.is_dir():
                dst = self / source.name
            else:
                dst = self

            dst._upload_file_to_cloud(source, force_overwrite_to_cloud=force_overwrite_to_cloud)

            return dst

    def _copy(
        self,
        target: Union[str, os.PathLike, "CloudPath"],
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
        remove_src: bool = False,
    ) -> Union[Path, Self]:
        if not self.exists():
            raise ValueError(f"Path {self} must exist to copy.")

        destination = anypath.to_anypath(target)

        if self.is_dir():
            result = self.copytree(
                destination,  # type: ignore[arg-type]
                force_overwrite_to_cloud=force_overwrite_to_cloud,
            )
            return cast(Union[Path, Self], result)

        if not isinstance(destination, CloudPath):
            return self.download_to(destination)

        # if same client, use cloud-native _move_file on client to avoid downloading
        if self.client is destination.client:
            if destination.exists() and destination.is_dir():
                destination = destination / self.name

            if force_overwrite_to_cloud is None:
                force_overwrite_to_cloud = os.environ.get(
                    "CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD", "False"
                ).lower() in ["1", "true"]

            if (
                not force_overwrite_to_cloud
                and destination.exists()
                and destination.stat().st_mtime >= self.stat().st_mtime
            ):
                raise OverwriteNewerCloudError(
                    f"File ({destination}) is newer than ({self}). "
                    f"To overwrite "
                    f"pass `force_overwrite_to_cloud=True`."
                )

            return cast(Self, self.client._move_file(self, destination, remove_src=remove_src))

        else:
            if not destination.exists() or destination.is_file():
                return cast(
                    Union[Path, Self],
                    destination.upload_from(
                        self.fspath, force_overwrite_to_cloud=force_overwrite_to_cloud
                    ),
                )
            else:
                return cast(
                    Union[Path, Self],
                    (destination / self.name).upload_from(
                        self.fspath, force_overwrite_to_cloud=force_overwrite_to_cloud
                    ),
                )

    @overload
    def copy(
        self,
        target: Self,
        follow_symlinks=True,
        preserve_metadata=False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Self: ...

    @overload
    def copy(
        self,
        target: Path,
        follow_symlinks=True,
        preserve_metadata=False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Path: ...

    @overload
    def copy(
        self,
        target: str,
        follow_symlinks=True,
        preserve_metadata=False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Union[Path, "CloudPath"]: ...

    def copy(
        self,
        target: Union[str, os.PathLike, Self],
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Union[Path, Self]:
        """Copy self to target folder or file, if self is a file."""
        return self._copy(
            target,
            follow_symlinks=follow_symlinks,
            preserve_metadata=preserve_metadata,
            force_overwrite_to_cloud=force_overwrite_to_cloud,
            remove_src=False,
        )

    @overload
    def copy_into(
        self,
        target_dir: Self,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Self: ...

    @overload
    def copy_into(
        self,
        target_dir: Path,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Path: ...

    @overload
    def copy_into(
        self,
        target_dir: str,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Union[Path, "CloudPath"]: ...

    def copy_into(
        self,
        target_dir: Union[str, os.PathLike, Self],
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Union[Path, Self]:
        """Copy self into target directory, preserving the filename."""
        target_path = anypath.to_anypath(target_dir) / self.name

        result = self._copy(
            target_path,
            follow_symlinks=follow_symlinks,
            preserve_metadata=preserve_metadata,
            force_overwrite_to_cloud=force_overwrite_to_cloud,
            remove_src=False,
        )
        return cast(Union[Path, Self], result)

    @overload
    def copytree(
        self,
        destination: Self,
        force_overwrite_to_cloud: Optional[bool] = None,
        ignore: Optional[Callable[[str, Iterable[str]], Container[str]]] = None,
    ) -> Self: ...

    @overload
    def copytree(
        self,
        destination: Path,
        force_overwrite_to_cloud: Optional[bool] = None,
        ignore: Optional[Callable[[str, Iterable[str]], Container[str]]] = None,
    ) -> Path: ...

    @overload
    def copytree(
        self,
        destination: Union[str, os.PathLike, Self],
        force_overwrite_to_cloud: Optional[bool] = None,
        ignore: Optional[Callable[[str, Iterable[str]], Container[str]]] = None,
    ) -> Union[Path, "CloudPath"]: ...

    def copytree(self, destination, force_overwrite_to_cloud=None, ignore=None):
        """Copy self to a directory, if self is a directory."""
        if not self.is_dir():
            raise CloudPathNotADirectoryError(
                f"Origin path {self} must be a directory. To copy a single file use the method copy."
            )

        destination = anypath.to_anypath(destination)

        if destination.exists() and destination.is_file():
            raise CloudPathFileExistsError(
                f"Destination path {destination} of copytree must be a directory."
            )

        contents = list(self.iterdir())

        if ignore is not None:
            ignored_names = ignore(self._no_prefix_no_drive, [x.name for x in contents])
        else:
            ignored_names = set()

        destination.mkdir(parents=True, exist_ok=True)

        for subpath in contents:
            if subpath.name in ignored_names:
                continue
            if subpath.is_file():
                subpath.copy(
                    destination / subpath.name, force_overwrite_to_cloud=force_overwrite_to_cloud
                )
            elif subpath.is_dir():
                subpath.copytree(
                    destination / (subpath.name + ("" if subpath.name.endswith("/") else "/")),
                    force_overwrite_to_cloud=force_overwrite_to_cloud,
                    ignore=ignore,
                )

        return destination

    @overload
    def move(
        self,
        target: Self,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Self: ...

    @overload
    def move(
        self,
        target: Path,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Path: ...

    @overload
    def move(
        self,
        target: str,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Union[Path, "CloudPath"]: ...

    def move(
        self,
        target: Union[str, os.PathLike, Self],
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Union[Path, Self]:
        """Move self to target location, removing the source."""
        return self._copy(
            target,
            follow_symlinks=follow_symlinks,
            preserve_metadata=preserve_metadata,
            force_overwrite_to_cloud=force_overwrite_to_cloud,
            remove_src=True,
        )

    @overload
    def move_into(
        self,
        target_dir: Self,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Self: ...

    @overload
    def move_into(
        self,
        target_dir: Path,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Path: ...

    @overload
    def move_into(
        self,
        target_dir: str,
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Union[Path, "CloudPath"]: ...

    def move_into(
        self,
        target_dir: Union[str, os.PathLike, Self],
        follow_symlinks: bool = True,
        preserve_metadata: bool = False,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Union[Path, Self]:
        """Move self into target directory, preserving the filename and removing the source."""
        target_path = anypath.to_anypath(target_dir) / self.name

        result = self._copy(
            target_path,
            follow_symlinks=follow_symlinks,
            preserve_metadata=preserve_metadata,
            force_overwrite_to_cloud=force_overwrite_to_cloud,
            remove_src=True,
        )
        return cast(Union[Path, Self], result)

    def clear_cache(self):
        """Removes cache if it exists"""
        if self._local.exists():
            if self._local.is_file():
                self._local.unlink()
            else:
                shutil.rmtree(self._local)

    # ===========  private cloud methods ===============
    @property
    def _local(self) -> Path:
        """Cached local version of the file."""
        return self.client._local_cache_dir / self._no_prefix

    def _new_cloudpath(self, path: Union[str, os.PathLike], *parts: str) -> Self:
        """Use the scheme, client, cache dir of this cloudpath to instantiate
        a new cloudpath of the same type with the path passed.

        Used to make results of iterdir and joins have a unified client + cache.
        """
        path = str(path)

        # strip initial "/" if path has one
        if path.startswith("/"):
            path = path[1:]

        # add prefix/anchor if it is not already
        if not path.startswith(self.anchor):
            path = f"{self.anchor}{path}"

        return self.client.CloudPath(path, *parts)

    def _refresh_cache(self, force_overwrite_from_cloud: Optional[bool] = None) -> None:
        try:
            stats = self.stat()
        except NoStatError:
            # nothing to cache if the file does not exist; happens when creating
            # new files that will be uploaded
            return

        if force_overwrite_from_cloud is None:
            force_overwrite_from_cloud = os.environ.get(
                "CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD", "False"
            ).lower() in ["1", "true"]

        # if not exist or cloud newer
        if (
            force_overwrite_from_cloud
            or not self._local.exists()
            or (self._local.stat().st_mtime < stats.st_mtime)
        ):
            # ensure there is a home for the file
            self._local.parent.mkdir(parents=True, exist_ok=True)
            self.download_to(self._local)

            # force cache time to match cloud times
            os.utime(self._local, times=(stats.st_mtime, stats.st_mtime))

        if self._dirty:
            raise OverwriteDirtyFileError(
                f"Local file ({self._local}) for cloud path ({self}) has been changed by your code, but "
                f"is being requested for download from cloud. Either (1) push your changes to the cloud, "
                f"(2) remove the local file, or (3) pass `force_overwrite_from_cloud=True` to "
                f"overwrite; or set env var CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD=1."
            )

        # if local newer but not dirty, it was updated
        # by a separate process; do not overwrite unless forced to
        if self._local.stat().st_mtime > stats.st_mtime:
            raise OverwriteNewerLocalError(
                f"Local file ({self._local}) for cloud path ({self}) is newer on disk, but "
                f"is being requested for download from cloud. Either (1) push your changes to the cloud, "
                f"(2) remove the local file, or (3) pass `force_overwrite_from_cloud=True` to "
                f"overwrite; or set env var CLOUDPATHLIB_FORCE_OVERWRITE_FROM_CLOUD=1."
            )

    def _upload_local_to_cloud(
        self,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Self:
        """Uploads cache file at self._local to the cloud"""
        # We should never try to be syncing entire directories; we should only
        # cache and upload individual files.
        if self._local.is_dir():
            raise ValueError("Only individual files can be uploaded to the cloud")

        uploaded = self._upload_file_to_cloud(
            self._local, force_overwrite_to_cloud=force_overwrite_to_cloud
        )

        # force cache time to match cloud times
        stats = self.stat()
        os.utime(self._local, times=(stats.st_mtime, stats.st_mtime))

        # reset dirty and handle now that this is uploaded
        self._dirty = False
        self._handle = None

        return uploaded

    def _upload_file_to_cloud(
        self,
        local_path: Path,
        force_overwrite_to_cloud: Optional[bool] = None,
    ) -> Self:
        """Uploads file at `local_path` to the cloud if there is not a newer file
        already there.
        """
        if force_overwrite_to_cloud is None:
            force_overwrite_to_cloud = os.environ.get(
                "CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD", "False"
            ).lower() in ["1", "true"]

        if force_overwrite_to_cloud:
            # If we are overwriting no need to perform any checks, so we can save time
            self.client._upload_file(
                local_path,
                self,
            )
            return self

        try:
            stats = self.stat()
        except NoStatError:
            stats = None

        # if cloud does not exist or local is newer, do the upload
        if not stats or (local_path.stat().st_mtime > stats.st_mtime):
            self.client._upload_file(
                local_path,
                self,
            )

            return self

        # cloud is newer and we are not overwriting
        raise OverwriteNewerCloudError(
            f"Local file ({self._local}) for cloud path ({self}) is newer in the cloud disk, but "
            f"is being requested to be uploaded to the cloud. Either (1) redownload changes from the cloud or "
            f"(2) pass `force_overwrite_to_cloud=True` to "
            f"overwrite; or set env var CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD=1."
        )

    # ===========  pydantic integration special methods ===============
    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler):
        """Pydantic special method. See
        https://docs.pydantic.dev/2.0/usage/types/custom/"""
        try:
            from pydantic_core import core_schema

            return core_schema.no_info_after_validator_function(
                cls.validate,
                core_schema.any_schema(),
            )
        except ImportError:
            return None

    @classmethod
    def validate(cls, v: str) -> Self:
        """Used as a Pydantic validator. See
        https://docs.pydantic.dev/2.0/usage/types/custom/"""
        return cls(v)

    @classmethod
    def __get_validators__(cls) -> Generator[Callable[[Any], Self], None, None]:
        """Pydantic special method. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        yield cls._validate

    @classmethod
    def _validate(cls, value: Any) -> Self:
        """Used as a Pydantic validator. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        return cls(value)


# The function resolve is not available on Pure paths because it removes relative
# paths and symlinks. We _just_ want the relative path resolution for
# cloud paths, so the other logic is removed.  Also, we can assume that
# cloud paths are absolute.
#
# Based on resolve from pathlib:
# https://github.com/python/cpython/blob/3.8/Lib/pathlib.py#L316-L359
def _resolve(path: PurePosixPath) -> str:
    sep = "/"

    # rebuild path from parts
    newpath = ""
    for name in str(path).split(sep):
        if not name or name == ".":
            # current dir, nothing to add
            continue
        if name == "..":
            # parent dir, drop right-most part
            newpath, _, _ = newpath.rpartition(sep)
            continue
        newpath = newpath + sep + name

    return newpath or sep


# These objects are used to wrap CloudPaths in a context where we can use
# the python pathlib implementations for `glob` and `rglob`, which depend
# on the Selector created by the `_make_selector` method being passed
# an object like the below when `select_from` is called. We implement these methods
# in a simple wrapper to use the same glob recursion and pattern logic without
# rolling our own.
#
# Designed to be compatible when used by these selector implementations from pathlib:
# https://github.com/python/cpython/blob/3.10/Lib/pathlib.py#L385-L500
class _CloudPathSelectableAccessor:
    def __init__(self, scandir_func: Callable) -> None:
        self.scandir = scandir_func


class _CloudPathSelectable:
    def __init__(
        self,
        name: str,
        parents: List[str],
        children: Any,  # Nested dictionaries as tree
        exists: bool = True,
    ) -> None:
        self._name = name
        self._all_children = children
        self._parents = parents
        self._exists = exists

        self._accessor = _CloudPathSelectableAccessor(self.scandir)

    def __repr__(self) -> str:
        return "/".join(self._parents + [self.name])

    def is_dir(self, follow_symlinks: bool = False) -> bool:
        return self._all_children is not None

    def exists(self) -> bool:
        return self._exists

    def is_symlink(self) -> bool:
        return False

    @property
    def name(self) -> str:
        return self._name

    def _make_child_relpath(self, part):
        # pathlib internals shortcut; makes a relative path, even if it doesn't actually exist
        return _CloudPathSelectable(
            part,
            self._parents + [self.name],
            self._all_children.get(part, None),
            exists=part in self._all_children,
        )

    @staticmethod
    @contextmanager
    def scandir(
        root: "_CloudPathSelectable",
    ) -> Generator[Generator["_CloudPathSelectable", None, None], None, None]:
        yield (
            _CloudPathSelectable(child, root._parents + [root._name], grand_children)
            for child, grand_children in root._all_children.items()
        )

    _scandir = scandir  # Py 3.11 compatibility

    def walk(self):
        # split into dirs and files
        dirs_files = defaultdict(list)
        with self.scandir(self) as items:
            for child in items:
                dirs_files[child.is_dir()].append(child)

            # top-down, so yield self before recursive call
            yield self, [f.name for f in dirs_files[True]], [f.name for f in dirs_files[False]]

            for child_dir in dirs_files[True]:
                yield from child_dir.walk()
