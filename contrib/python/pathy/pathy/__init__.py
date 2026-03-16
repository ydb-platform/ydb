import importlib
import os
import pathlib
import shutil
import tempfile
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from io import DEFAULT_BUFFER_SIZE
from pathlib import Path, PurePath
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import smart_open
import smart_open.compression
from pathlib_abc import PathBase, PurePathBase

from . import pathmod as pathy_pathmod

SUBCLASS_ERROR = "must be implemented in a subclass"

StreamableType = IO[Any]
FluidPath = Union["Pathy", "PathlibPathEx"]
BucketType = TypeVar("BucketType")
BucketBlobType = TypeVar("BucketBlobType")

_drive_letters = [(f"{chr(ord('a') + i)}:") for i in range(26)]  # lower case with :


@dataclass
class ClientError(BaseException):
    message: str
    code: Optional[int]

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"({self.code}) {self.message}"


@dataclass
class BlobStat:
    """Stat for a bucket item"""

    name: str
    size: Optional[int]
    last_modified: Optional[int]


@dataclass
class Blob:
    bucket: Any
    name: str
    size: Optional[int]
    updated: Optional[int]
    owner: Optional[str]
    raw: Any

    def delete(self) -> None:
        raise NotImplementedError(SUBCLASS_ERROR)

    def exists(self) -> bool:
        raise NotImplementedError(SUBCLASS_ERROR)


class BucketEntry:
    """A single item returned from scanning a path"""

    name: str
    _is_dir: bool
    _stat: BlobStat
    raw: Optional[Blob]

    def __init__(
        self,
        name: str,
        is_dir: bool = False,
        size: int = -1,
        last_modified: Optional[int] = None,
        raw: Optional[Blob] = None,
    ):
        self.name = name
        self.raw = raw
        self._is_dir = is_dir
        self._stat = BlobStat(name=name, size=size, last_modified=last_modified)

    def __repr__(self) -> str:
        return "{}(name={}, is_dir={}, stat={})".format(
            type(self).__name__, self.name, self._is_dir, self._stat
        )

    def inode(self, *args: Any, **kwargs: Dict[str, Any]) -> None:
        return None

    def is_dir(self, follow_symlinks: bool = True) -> bool:
        return self._is_dir

    def is_file(self, follow_symlinks: bool = True) -> bool:
        return not self._is_dir

    def stat(self, *, follow_symlinks: bool = True) -> BlobStat:
        return self._stat


@dataclass
class Bucket:
    def get_blob(self, blob_name: str) -> Optional[Blob]:
        raise NotImplementedError(SUBCLASS_ERROR)

    def copy_blob(self, blob: Blob, target: "Bucket", name: str) -> Optional[Blob]:
        raise NotImplementedError(SUBCLASS_ERROR)

    def delete_blob(self, blob: Blob) -> None:
        raise NotImplementedError(SUBCLASS_ERROR)

    def delete_blobs(self, blobs: List[Blob]) -> None:
        raise NotImplementedError(SUBCLASS_ERROR)

    def exists(self) -> bool:
        raise NotImplementedError(SUBCLASS_ERROR)


class BucketClient:
    """Base class for a client that interacts with a bucket-based storage system."""

    def get_blob(self, path: "Pathy") -> Optional[Blob]:
        """Get the blob associated with a path or return None"""
        bucket = self.lookup_bucket(path)
        if bucket is None:
            return None
        return bucket.get_blob(path.key)

    def recreate(self, **kwargs: Any) -> None:
        """Recreate any underlying bucket client adapter with the given kwargs"""

    def open(
        self,
        path: "Pathy",
        *,
        mode: str = "r",
        buffering: int = DEFAULT_BUFFER_SIZE,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> StreamableType:
        client_params = {}
        if hasattr(self, "client_params"):
            client_params = getattr(self, "client_params")

        return smart_open.open(
            self.make_uri(path),
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            transport_params=client_params,
            # Disable de/compression based on the file extension
            compression=smart_open.compression.NO_COMPRESSION,
        )  # type:ignore

    def make_uri(self, path: "Pathy") -> str:
        return str(path)

    def is_dir(self, path: "Pathy") -> bool:
        return any(self.list_blobs(path, prefix=path.prefix))

    def rmdir(self, path: "Pathy") -> None:
        return None

    def exists(self, path: "Pathy") -> bool:
        raise NotImplementedError(SUBCLASS_ERROR)

    def lookup_bucket(self, path: "Pathy") -> Optional[Bucket]:
        return None

    def get_bucket(self, path: "Pathy") -> Bucket:
        raise NotImplementedError(SUBCLASS_ERROR)

    def list_blobs(
        self,
        path: "Pathy",
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> Generator[Blob, None, None]:
        raise NotImplementedError(SUBCLASS_ERROR)

    def scandir(
        self,
        path: "Pathy",
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> "PathyScanDir":
        raise NotImplementedError(SUBCLASS_ERROR)

    def create_bucket(self, path: "Pathy") -> Bucket:
        raise NotImplementedError(SUBCLASS_ERROR)

    def delete_bucket(self, path: "Pathy") -> None:
        raise NotImplementedError(SUBCLASS_ERROR)

    def owner(self, path: "Pathy") -> Optional[str]:
        blob: Optional[Blob] = self.get_blob(path)
        return blob.owner if blob is not None else None

    def resolve(self, path: "Pathy", strict: bool = False) -> "Pathy":
        scheme, bucket, parts = pathy_pathmod.splitroot(str(path), resolve=True)
        resolved = (
            f"{scheme}{pathy_pathmod.schemesep}{bucket}{pathy_pathmod.sep}{parts}"
        )
        # On Windows the abspath normalization that happens replaces
        # / with \\ so we have to revert it here to preserve the
        # expected resolved path.
        if path.drive.lower() not in _drive_letters:
            resolved = resolved.replace("\\", "/")
        return Pathy(resolved)

    def mkdir(self, path: "Pathy", mode: int = 0) -> None:
        bucket: Optional[Bucket] = self.lookup_bucket(path)
        if bucket is None or not bucket.exists():
            self.create_bucket(path)


class PurePathy(PurePathBase):
    """PurePathBase subclass for bucket storage."""

    pathmod = pathy_pathmod
    __slots__ = ()

    @property
    def scheme(self) -> str:
        """Return the scheme portion of this path. A path's scheme is the leading
        few characters. In a website you would see a scheme of "http" or "https".

        Consider a few examples:

        ```python
        assert Pathy("gs://foo/bar").scheme == "gs"
        assert Pathy("file:///tmp/foo/bar").scheme == "file"
        """
        # If there is no drive, return nothing
        if self.drive == "":
            return ""
        return str(self.drive)

    @property
    def bucket(self) -> "Pathy":
        """Return a new instance of only the bucket path."""
        self._absolute_path_validation()
        return cast(Pathy, type(self)(f"{self.drive}://{self.root}"))

    @property
    def key(self) -> str:
        """Return a new instance of only the key path."""
        self._absolute_path_validation()
        if len(self._tail) == 0:
            return ""
        return pathy_pathmod.sep.join(self._tail)

    @property
    def prefix(self) -> str:
        """Returns part of the path after the bucket-name, always ending with path.sep,
        or an empty string if there is no prefix."""
        if self.key == "":
            return ""
        return f"{self.key}{pathy_pathmod.sep}"

    @property
    def parts(self) -> Tuple[str, ...]:
        """An object providing sequence-like access to the components in the cloud
        bucket path.

        Returns a tuple of the scheme, bucket, path parts

        If the scheme or bucket aren't set, they will be empty strings.
        """
        return (self.drive, self.root) + tuple(self._tail)  # type: ignore

    def __bytes__(self) -> bytes:
        """Return the bytes representation of the path.  This is only
        recommended to use under Unix."""
        return os.fsencode(f"{self}")

    def _absolute_path_validation(self) -> None:
        if not self.is_absolute():
            raise ValueError("relative paths are unsupported")

    def __repr__(self) -> str:
        return "{}({!r})".format(self.__class__.__name__, self.as_posix())

    def __lt__(self, other: object) -> bool:
        """Support < comparisons and sorting with sorted()"""
        return isinstance(other, (PurePath, Pathy)) and self.parts < other.parts

    def __fspath__(self) -> str:
        """Compatibilty with os.fspath()

        ```python
        import os
        from pathy import Pathy

        path: Pathy = Pathy("gs://foo/bar")
        str_path: str = os.fspath(path)
        ```
        """
        return str(self)

    def __eq__(self, other: object) -> bool:
        """Enables equality comparisons, e.g.

        ```python
        from pathy import Pathy

        Pathy("gs://foo/bar") == Pathy("gs://foo/bar")
        ```
        """
        return (
            isinstance(other, PurePathy)
            and self.parts == other.parts
            and self.scheme == other.scheme
            and self.drive == other.drive
        )

    def as_posix(self) -> str:
        return self._format_parsed_parts(self.drive, self.root, self._tail)

    @classmethod
    def _format_parsed_parts(cls, drv: str, root: str, tail: List[str]) -> str:
        # Bucket path "gs://foo/bar"
        join_fn: Callable[[List[str]], str] = pathy_pathmod.sep.join
        # If the scheme is file and it's Windows, use \\ slashes to join
        if drv.lower() == "file" and os.name == "nt":
            join_fn = "\\".join
        if drv != "" or root != "":
            res = f"{drv}{pathy_pathmod.schemesep}{root}{pathy_pathmod.sep}" + join_fn(
                tail
            )
        else:
            res = join_fn(tail)
        return res


_SUPPORTED_OPEN_MODES = {"r", "rb", "tr", "rt", "w", "wb", "bw", "wt", "tw"}

if os.name == "nt":
    BasePathlibPath = pathlib.WindowsPath  # type:ignore
else:
    BasePathlibPath = pathlib.PosixPath  # type:ignore


class PathlibPathEx(BasePathlibPath):
    """Extension of pathlib.Path that includes Pathy helpers. This makes using Pathy
    consistent across cloud and pathlib objects, including the expanded API."""

    def ls(self) -> Generator[BlobStat, None, None]:
        for entry in self.iterdir():
            stat = entry.stat()
            yield BlobStat(
                name=entry.name, size=stat.st_size, last_modified=int(stat.st_mtime)
            )


class BasePath(PathBase):
    def ls(self: Any) -> Generator["BlobStat", None, None]:
        client: BucketClient = get_client(getattr(self, "scheme", "file"))
        blobs: "ScanDirFS" = cast(
            ScanDirFS, client.scandir(self, prefix=getattr(self, "prefix", None))
        )
        for blob in blobs:
            stat = blob.stat()
            yield BlobStat(
                name=str(blob.name), size=stat.size, last_modified=stat.last_modified
            )

    def iterdir(self: Any) -> Generator["BasePath", None, None]:
        """Iterate over the blobs found in the given bucket or blob prefix path."""
        client: BucketClient = get_client(getattr(self, "scheme", "file"))
        blobs: "ScanDirFS" = cast(
            ScanDirFS, client.scandir(self, prefix=getattr(self, "prefix", None))
        )  # type:ignore
        for blob in blobs:
            yield self / blob.name


class Pathy(PurePathy, BasePath):
    """Subclass of `PathBase` that works with bucket APIs."""

    __slots__ = ()
    _NOT_SUPPORTED_MESSAGE = "{method} is an unsupported bucket operation"
    _client: Optional[BucketClient]

    def client(self, path: "Pathy") -> BucketClient:
        return get_client(path.scheme)

    def __truediv__(self, key: Union[str, PathBase, "Pathy", PurePathy]) -> "Pathy":
        return super().__truediv__(key)  # type:ignore

    def _make_child_entry(self, entry: BucketEntry) -> "Pathy":
        # Transform an entry yielded from _scandir() into a path object.
        return self / Pathy(entry.name)

    @classmethod
    def fluid(cls, path_candidate: Union[str, FluidPath]) -> FluidPath:
        """Infer either a Pathy or pathlib.Path from an input path or string.

        The returned type is a union of the potential `FluidPath` types and will
        type-check correctly against the minimum overlapping APIs of all the input
        types.

        If you need to use specific implementation details of a type, "narrow" the
        return of this function to the desired type, e.g.

        ```python
        from pathlib import Path
        from pathy import FluidPath, Pathy

        fluid_path: FluidPath = Pathy.fluid("gs://my_bucket/foo.txt")
        # Narrow the type to a specific class
        assert isinstance(fluid_path, Pathy), "must be Pathy"
        # Use a member specific to that class
        assert fluid_path.prefix == "foo.txt/"

        # Or use a file-system path
        posix_path: FluidPath = Pathy.fluid("/tmp/foo.txt")
        assert isinstance(posix_path, Path), "must be pathlib.Path"
        ```
        """
        result = Pathy(path_candidate)
        if result.root != "":
            return result
        return PathlibPathEx(str(path_candidate))

    @classmethod
    def from_bucket(cls, bucket_name: str, scheme: str = "gs") -> "Pathy":
        """Initialize a Pathy from a bucket name. This helper adds a trailing slash and
        the appropriate prefix.

        ```python
        from pathy import Pathy

        assert str(Pathy.from_bucket("one")) == "gs://one/"
        assert str(Pathy.from_bucket("two")) == "gs://two/"
        ```
        """
        return Pathy(f"{scheme}://{bucket_name}/")

    @classmethod
    def to_local(cls, blob_path: Union["Pathy", str], recurse: bool = True) -> Path:
        """Download and cache either a blob or a set of blobs matching a prefix.

        The cache is sensitive to the file updated time, and downloads new blobs
        as their updated timestamps change."""
        cache_folder = get_fs_cache()
        if cache_folder is None:
            raise ValueError(
                'cannot get and cache a blob without first calling "use_fs_cache"'
            )

        cache_folder.mkdir(exist_ok=True, parents=True)

        in_path: Pathy
        if not isinstance(blob_path, Pathy):
            in_path = Pathy(blob_path)
        else:
            in_path = blob_path

        cache_blob: Path = cache_folder.absolute() / in_path.root
        if in_path.key != "":
            cache_blob /= in_path.key
        cache_time: Path = (
            cache_folder.absolute() / in_path.root / f"{in_path.key}.time"
        )
        # Keep a cache of downloaded files. Fetch new ones when:
        #  - the file isn't in the cache
        #  - cached_stat.updated != latest_stat.updated
        if cache_blob.exists() and cache_time.exists():
            fs_time: str = cache_time.read_text()
            gcs_stat: BlobStat = in_path.stat()
            # If the times match, return the cached blob
            if fs_time == str(gcs_stat.last_modified):
                return cache_blob
            # remove the cache files because they're out of date
            cache_blob.unlink()
            cache_time.unlink()

        # If the file isn't in the cache, download it
        if not cache_blob.exists():
            # Is a blob
            if in_path.is_file():
                dest_folder = cache_blob.parent
                dest_folder.mkdir(exist_ok=True, parents=True)
                cache_blob.write_bytes(in_path.read_bytes())
                blob_stat: BlobStat = in_path.stat()
                cache_time.write_text(str(blob_stat.last_modified))
            elif recurse:
                # If not a specific blob, enumerate all the blobs under
                # the path and cache them, then return the cache folder
                for blob in in_path.rglob("*"):
                    Pathy.to_local(blob, recurse=False)
        return cache_blob

    def ls(self: "Pathy") -> Generator["BlobStat", None, None]:
        """List blob names with stat information under the given path.

        This is considerably faster than using iterdir if you also need
        the stat information for the enumerated blobs.

        Yields BlobStat objects for each found blob.
        """
        yield from super().ls()

    def stat(  # type: ignore[override]
        self: "Pathy", *, follow_symlinks: bool = True
    ) -> BlobStat:
        """Returns information about this bucket path."""
        self._absolute_path_validation()
        if self.key == "":
            raise ValueError("cannot stat a bucket without a key")
        bucket = self.client(self).get_bucket(self)
        blob: Optional[Blob] = bucket.get_blob(self.key)
        if blob is None:
            raise FileNotFoundError(self)
        return BlobStat(name=str(blob.name), size=blob.size, last_modified=blob.updated)

    def exists(self: "Pathy") -> bool:
        """Returns True if the path points to an existing bucket, blob, or prefix."""
        self._absolute_path_validation()
        client = self.client(self)
        bucket = client.lookup_bucket(self)
        if bucket is None or not bucket.exists():
            return False
        if self.key == "":
            return True

        blob: Optional[Blob] = bucket.get_blob(self.key)
        if blob is not None:
            return blob.exists()
        # Determine if the path exists according to the current adapter
        return client.exists(self)

    def is_dir(self: "Pathy", *, follow_symlinks: bool = True) -> bool:
        """Determine if the path points to a bucket or a prefix of a given blob
        in the bucket.

        Returns True if the path points to a bucket or a blob prefix.
        Returns False if it points to a blob or the path doesn't exist.
        """
        self._absolute_path_validation()
        if self.bucket and self.key == "":
            return self.client(self).lookup_bucket(self) is not None
        return self.client(self).is_dir(self)

    def is_file(self: "Pathy", *, follow_symlinks: bool = True) -> bool:
        """Determine if the path points to a blob in the bucket.

        Returns True if the path points to a blob.
        Returns False if it points to a bucket or blob prefix, or if the path doesn’t
        exist.
        """
        self._absolute_path_validation()
        if not self.bucket or self.key == "":
            return False
        try:
            return bool(self.stat(follow_symlinks=follow_symlinks))
        except (ClientError, FileNotFoundError):
            return False

    def glob(
        self: "Pathy",
        pattern: str,
        *,
        case_sensitive: Optional[bool] = None,
        follow_symlinks: Optional[bool] = None,
    ) -> Generator["Pathy", None, None]:
        """Perform a glob match relative to this Pathy instance, yielding all matched
        blobs."""
        yield from super().glob(pattern)

    def open(  # type:ignore
        self: "Pathy",
        mode: str = "r",
        buffering: int = DEFAULT_BUFFER_SIZE,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> StreamableType:
        """Open the given blob for streaming. This delegates to the `smart_open`
        library that handles large file streaming for a number of bucket API
        providers."""
        self._absolute_path_validation()
        if mode not in _SUPPORTED_OPEN_MODES:
            raise ValueError(
                "supported modes are {} got {}".format(_SUPPORTED_OPEN_MODES, mode)
            )
        if buffering == 0 or buffering == 1:
            raise ValueError(
                "supported buffering values are only block sizes, no 0 or 1"
            )
        if "b" in mode and encoding:
            raise ValueError("binary mode doesn't take an encoding argument")

        return self.client(self).open(
            self,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    def owner(self: "Pathy") -> Optional[str]:  # type:ignore[override]
        """Returns the name of the user that owns the bucket or blob
        this path points to. Returns None if the owner is unknown or
        not supported by the bucket API provider."""
        self._absolute_path_validation()
        if not self.is_file():
            raise FileNotFoundError(str(self))
        return self.client(self).owner(self)

    def resolve(self, strict: bool = False) -> "Pathy":
        """Resolve the given path to remove any relative path specifiers.

        ```python
        from pathy import Pathy

        path = Pathy("gs://my_bucket/folder/../blob")
        assert path.resolve() == Pathy("gs://my_bucket/blob")
        ```
        """
        self._absolute_path_validation()
        return self.client(self).resolve(self, strict=strict)

    def rename(  # type:ignore
        self: "Pathy", target: Union[str, PurePathBase]
    ) -> "Pathy":
        """Rename this path to the given target.

        If the target exists and is a file, it will be replaced silently if the user
        has permission.

        If path is a blob prefix, it will replace all the blobs with the same prefix
        to match the target prefix."""
        self._absolute_path_validation()
        self_type = type(self)
        result = target if isinstance(target, self_type) else self_type(target)
        result._absolute_path_validation()  # type:ignore

        client: BucketClient = self.client(self)
        bucket: Bucket = client.get_bucket(self)
        target_bucket: Bucket = client.get_bucket(result)

        # Single file
        if not self.is_dir():
            from_blob: Optional[Blob] = bucket.get_blob(self.key)
            if from_blob is None:
                raise FileNotFoundError(f'source file "{self}" does not exist')
            target_bucket.copy_blob(from_blob, target_bucket, result.key)
            bucket.delete_blob(from_blob)
            return self

        # Folder with objects
        sep: str = pathy_pathmod.sep
        blobs = list(client.list_blobs(self, prefix=self.prefix, delimiter=sep))
        # First rename
        for blob in blobs:
            target_key_name = blob.name.replace(self.key, result.key)
            target_bucket.copy_blob(blob, target_bucket, target_key_name)
        # Then delete the sources
        for blob in blobs:
            bucket.delete_blob(blob)
        return self

    def replace(self: "Pathy", target: Union[str, PurePathBase]) -> "Pathy":
        """Renames this path to the given target.

        If target points to an existing path, it will be replaced."""
        return self.rename(target)

    def rmdir(self: "Pathy") -> None:
        """Removes this bucket or blob prefix. It must be empty."""
        self._absolute_path_validation()
        if self.is_file():
            raise NotADirectoryError()
        if not self.is_dir():
            raise FileNotFoundError()
        client: BucketClient = self.client(self)
        key_name = self.key if self.key != "" else None
        bucket: Bucket = client.get_bucket(self)
        blobs = list(client.list_blobs(self, prefix=key_name))
        bucket.delete_blobs(blobs)
        # The path is just the bucket
        if key_name is None:
            client.delete_bucket(self)
        elif client.is_dir(self):
            client.rmdir(self)

    def samefile(self: "Pathy", other_path: Union[str, bytes, int, PathBase]) -> bool:
        """Determine if this path points to the same location as other_path."""
        self._absolute_path_validation()
        if not isinstance(other_path, PathBase):
            other_path = type(self)(other_path)  # type:ignore
        assert isinstance(other_path, Pathy)
        return (
            self.bucket == other_path.bucket
            and self.key == other_path.key
            and self.is_file()
        )

    def touch(self: "Pathy", mode: int = 0o666, exist_ok: bool = True) -> None:
        """Create a blob at this path.

        If the blob already exists, the function succeeds if exist_ok is true
        (and its modification time is updated to the current time), otherwise
        FileExistsError is raised.
        """
        if self.exists() and not exist_ok:
            raise FileExistsError()
        self.write_text("")

    def mkdir(
        self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create a bucket from the given path. Since bucket APIs only have implicit
        folder structures (determined by the existence of a blob with an overlapping
        prefix) this does nothing other than create buckets.

        If parents is False, the bucket will only be created if the path points to
        exactly the bucket and nothing else. If parents is true the bucket will be
        created even if the path points to a specific blob.

        The mode param is ignored.

        Raises FileExistsError if exist_ok is false and the bucket already exists.
        """
        try:
            # If the path is just the bucket, respect the result of "bucket.exists()"
            if self.key == "" and not exist_ok and self.bucket.exists():
                raise FileExistsError("Bucket {} already exists".format(self.bucket))
            return self.client(self).mkdir(self, mode)
        except OSError:
            if not exist_ok:
                raise

    def is_mount(self: "Pathy") -> bool:
        return False

    def is_socket(self: "Pathy") -> bool:
        return False

    def is_symlink(self: "Pathy") -> bool:
        return False

    def is_fifo(self: "Pathy") -> bool:
        return False

    def unlink(self: "Pathy") -> None:  # type:ignore[override]
        """Delete a link to a blob in a bucket."""
        bucket = self.client(self).get_bucket(self)
        blob: Optional[Blob] = bucket.get_blob(self.key)
        if blob is None:
            raise FileNotFoundError(self)
        blob.delete()

    # Unsupported operations below here

    @classmethod
    def cwd(cls) -> "Pathy":
        message = cls._NOT_SUPPORTED_MESSAGE.format(method=cls.cwd.__qualname__)
        raise NotImplementedError(message)

    @classmethod
    def home(cls) -> "Pathy":
        message = cls._NOT_SUPPORTED_MESSAGE.format(method=cls.home.__qualname__)
        raise NotImplementedError(message)

    def chmod(self: "Pathy", mode: int, *, follow_symlinks: bool = True) -> None:
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.chmod.__qualname__)
        raise NotImplementedError(message)

    def expanduser(self: "Pathy") -> "Pathy":
        message = self._NOT_SUPPORTED_MESSAGE.format(
            method=self.expanduser.__qualname__
        )
        raise NotImplementedError(message)

    def lchmod(self: "Pathy", mode: int) -> None:
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.lchmod.__qualname__)
        raise NotImplementedError(message)

    def group(self: "Pathy") -> str:
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.group.__qualname__)
        raise NotImplementedError(message)

    def is_block_device(self: "Pathy") -> bool:
        message = self._NOT_SUPPORTED_MESSAGE.format(
            method=self.is_block_device.__qualname__
        )
        raise NotImplementedError(message)

    def is_char_device(self: "Pathy") -> bool:
        message = self._NOT_SUPPORTED_MESSAGE.format(
            method=self.is_char_device.__qualname__
        )
        raise NotImplementedError(message)

    def lstat(self: "Pathy") -> os.stat_result:
        message = self._NOT_SUPPORTED_MESSAGE.format(method=self.lstat.__qualname__)
        raise NotImplementedError(message)

    def symlink_to(
        self, target: Union[str, PathBase], target_is_directory: bool = False
    ) -> None:
        message = self._NOT_SUPPORTED_MESSAGE.format(
            method=self.symlink_to.__qualname__
        )
        raise NotImplementedError(message)


class PathyScanDir(Iterator[Any], ABC):
    """A scandir implementation that works for all python 3.x versions."""

    def __init__(
        self,
        client: BucketClient,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._client = client
        self._path = path
        self._prefix = prefix
        self._delimiter = delimiter
        self._generator = self.scandir()

    def __next__(self) -> Generator[BucketEntry, None, None]:
        yield from self._generator

    def __iter__(self) -> Generator[BucketEntry, None, None]:
        yield from self._generator

    @abstractmethod
    def scandir(self) -> Generator[BucketEntry, None, None]:
        raise NotImplementedError("must be implemented in a subclass")


#
# File system adapter
#


class BucketEntryFS(BucketEntry):
    ...


@dataclass
class BlobFS(Blob):
    def delete(self) -> None:
        """Delete a file-based blob."""
        file_folder: str = os.path.dirname(self.raw)
        self.raw.unlink()
        # NOTE: in buckets folders only exist if a file is contained in them. Mimic
        # that behavior here by removing empty folders when the last file is removed.
        if len(os.listdir(file_folder)) == 0:
            shutil.rmtree(file_folder)

    def exists(self) -> bool:
        return self.raw.exists()


@dataclass
class BucketFS(Bucket):
    name: str
    bucket: PathlibPathEx

    def get_blob(self, blob_name: str) -> Optional[BlobFS]:  # type:ignore[override]
        native_blob = self.bucket / blob_name
        if not native_blob.exists() or native_blob.is_dir():
            return None
        stat = native_blob.stat()
        # path.owner() raises KeyError if the owner's UID isn't known
        #
        # https://docs.python.org/3/library/pathlib.html#Path.owner
        owner: Optional[str]
        try:
            # path.owner() raises NotImplementedError on windows
            owner = native_blob.owner()  # type:ignore
        except (KeyError, NotImplementedError):
            owner = None
        return BlobFS(
            bucket=self,
            owner=owner,
            name=blob_name,
            raw=native_blob,
            size=stat.st_size,
            updated=int(round(stat.st_mtime)),
        )

    def copy_blob(  # type:ignore[override]
        self, blob: BlobFS, target: "BucketFS", name: str
    ) -> Optional[BlobFS]:
        in_file = str(blob.bucket.bucket / blob.name)
        out_file = str(target.bucket / name)
        out_path = PathlibPathEx(os.path.dirname(out_file))
        if not out_path.exists():
            out_path.mkdir(parents=True)
        shutil.copy(in_file, out_file)
        return None

    def delete_blob(self, blob: BlobFS) -> None:  # type:ignore[override]
        blob.delete()

    def delete_blobs(self, blobs: List[BlobFS]) -> None:  # type:ignore[override]
        for blob in blobs:
            blob.delete()

    def exists(self) -> bool:
        return self.bucket.exists()


@dataclass
class BucketClientFS(BucketClient):
    # Root to store file-system buckets as children of
    root: PathlibPathEx = field(
        default_factory=lambda: PathlibPathEx(f"/tmp/pathy-{uuid.uuid4().hex}/")
    )

    def full_path(self, path: Pathy) -> PathlibPathEx:
        full_path: PathlibPathEx = self.root.absolute() / path.root
        if path.key != "":
            full_path = full_path / path.key
        return full_path

    def exists(self, path: Pathy) -> bool:
        """Return True if the path exists as a file or folder on disk"""
        return self.full_path(path).exists()

    def is_dir(self, path: Pathy) -> bool:
        return self.full_path(path).is_dir()

    def rmdir(self, path: Pathy) -> None:
        full_path = self.full_path(path)
        return shutil.rmtree(full_path)

    def mkdir(self, path: "Pathy", mode: int = 0) -> None:
        full_path = self.full_path(path)
        os.makedirs(full_path, exist_ok=True)

    def open(
        self,
        path: Pathy,
        *,
        mode: str = "r",
        buffering: int = DEFAULT_BUFFER_SIZE,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
    ) -> StreamableType:
        if self.lookup_bucket(path) is None:
            raise ClientError(message=f'bucket "{path.root}" does not exist', code=404)

        full_path = self.full_path(path)
        if not full_path.exists():
            if full_path.name != "":
                full_path = full_path.parent
            full_path.mkdir(parents=True, exist_ok=True)
        return super().open(
            path,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    def make_uri(self, path: PurePathy) -> str:
        if not path.root:
            raise ValueError(f"cannot make a URI to an invalid bucket: {path.root}")
        full_path = self.root.absolute() / path.root
        if path.key != "":
            full_path /= path.key
        result = f"file://{full_path}"
        return result

    def create_bucket(self, path: PurePathy) -> Bucket:
        if not path.root:
            raise ValueError(f"Invalid bucket name: {path.root}")
        bucket_path: PathBase = self.root / path.root
        if bucket_path.exists():
            raise FileExistsError(f"Bucket already exists at: {bucket_path}")
        bucket_path.mkdir(parents=True, exist_ok=True)
        return BucketFS(str(path.root), bucket=bucket_path)

    def delete_bucket(self, path: PurePathy) -> None:
        bucket_path: PathBase = self.root / str(path.root)
        if bucket_path.exists():
            shutil.rmtree(bucket_path)

    def lookup_bucket(self, path: PurePathy) -> Optional[BucketFS]:
        if path.root:
            bucket_path: PathlibPathEx = self.root / path.root
            if bucket_path.exists():
                return BucketFS(str(path.root), bucket=bucket_path)
        return None

    def get_bucket(self, path: PurePathy) -> BucketFS:
        if not path.root:
            raise ValueError(f"path has an invalid bucket_name: {path.root}")
        bucket_path: PathlibPathEx = self.root / path.root
        if bucket_path.is_dir():
            return BucketFS(str(path.root), bucket=bucket_path)
        raise FileNotFoundError(f"Bucket {path.root} does not exist!")

    def scandir(
        self,
        path: Pathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> PathyScanDir:
        return ScanDirFS(client=self, path=path, prefix=prefix, delimiter=delimiter)

    def list_blobs(
        self,
        path: PurePathy,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
    ) -> Generator[BlobFS, None, None]:
        assert path.root is not None
        bucket = self.get_bucket(path)
        scan_path = self.root / path.root
        if prefix is not None:
            scan_path = scan_path / prefix

        # PathBase to a file
        if scan_path.exists() and not scan_path.is_dir():
            stat = scan_path.stat()
            file_size = stat.st_size
            updated = int(round(stat.st_mtime_ns * 1000))
            yield BlobFS(
                bucket,
                name=str(scan_path),
                size=file_size,
                updated=updated,
                owner=None,
                raw=scan_path,
            )

        # Yield blobs for each file
        for file_path in scan_path.rglob("*"):
            if file_path.is_dir():
                continue
            stat = file_path.stat()
            file_size = stat.st_size
            updated = int(round(stat.st_mtime_ns * 1000))
            name = file_path.name
            if prefix is not None:
                name = prefix + name
            yield BlobFS(
                bucket,
                name=f"{prefix if prefix is not None else ''}{file_path.name}",
                size=file_size,
                updated=updated,
                owner=None,
                raw=file_path,
            )


class ScanDirFS(PathyScanDir):
    _client: BucketClientFS

    def scandir(self) -> Generator[BucketEntry, None, None]:
        scan_path = self._client.root / self._path.root
        if self._prefix is not None:
            scan_path = scan_path / self._prefix
        for dir_entry in scan_path.glob("*"):
            if dir_entry.is_dir():
                yield BucketEntryFS(dir_entry.name, is_dir=True, raw=None)
            else:
                file_path = PathlibPathEx(dir_entry)
                stat = file_path.stat()
                file_size = stat.st_size
                updated = int(round(stat.st_mtime))
                blob: BlobFS = BlobFS(
                    self._client.get_bucket(self._path),
                    name=dir_entry.name,
                    size=file_size,
                    updated=updated,
                    owner=None,
                    raw=file_path,
                )
                yield BucketEntryFS(
                    name=dir_entry.name,
                    is_dir=False,
                    size=file_size,
                    last_modified=updated,
                    raw=blob,
                )


#
# Client Registration
#


# The only built-in client is the file-system one
_client_registry: Dict[str, Type[BucketClient]] = {
    "": BucketClientFS,
    "file": BucketClientFS,
}
# Optional clients that we attempt to dynamically load when encountering
# a Pathy object with a matching scheme
_optional_clients: Dict[str, str] = {
    "gs": "pathy.gcs",
    "s3": "pathy.s3",
    "azure": "pathy.azure",
}
BucketClientType = TypeVar("BucketClientType", bound=BucketClient)

# Hold given client args for a scheme
_client_args_registry: Dict[str, Any] = {}
_instance_cache: Dict[str, Any] = {}
_fs_client: Optional["BucketClientFS"] = None
_fs_cache: Optional[PathlibPathEx] = None


def register_client(scheme: str, type: Type[BucketClient]) -> None:
    """Register a bucket client for use with certain scheme Pathy objects"""
    global _client_registry
    _client_registry[scheme] = type


def get_client(scheme: str) -> BucketClientType:  # type:ignore
    """Retrieve the bucket client for use with a given scheme"""
    global _client_registry, _instance_cache, _fs_client
    global _optional_clients, _client_args_registry
    if _fs_client is not None:
        return _fs_client  # type: ignore
    if scheme in _instance_cache:
        return _instance_cache[scheme]

    # Attempt to dynamically load optional clients if we find a matching scheme
    if scheme not in _client_registry and scheme in _optional_clients:
        importlib.import_module(_optional_clients[scheme])

    # Create the client from the known registry
    if scheme in _client_registry:
        kwargs: Dict[str, Any] = (
            _client_args_registry[scheme] if scheme in _client_args_registry else {}
        )
        _instance_cache[scheme] = _client_registry[scheme](**kwargs)  # type:ignore
        return _instance_cache[scheme]

    raise ValueError(f'There is no client registered to handle "{scheme}" paths')


def set_client_params(scheme: str, **kwargs: Any) -> None:
    """Specify args to pass when instantiating a service-specific Client
    object. This allows for passing credentials in whatever way your underlying
    client library prefers."""
    global _client_registry, _instance_cache, _client_args_registry
    _client_args_registry[scheme] = kwargs
    if scheme in _instance_cache:
        _instance_cache[scheme].recreate(**_client_args_registry[scheme])
    return None


def use_fs(
    root: Optional[Union[str, PathBase, PathlibPathEx, bool]] = None
) -> Optional[BucketClientFS]:
    """Use a path in the local file-system to store blobs and buckets.

    This is useful for development and testing situations, and for embedded
    applications."""
    global _fs_client
    # False - disable adapter
    if root is False:
        _fs_client = None
        return None

    # None or True - enable FS adapter with default root
    if root is None or root is True:
        # Look up "data" folder of pathy package similar to spaCy
        client_root = PathlibPathEx(__file__).parent / "data"
    else:
        assert isinstance(
            root, (str, PathBase, PathlibPathEx)
        ), f"root is not a known type: {type(root)}"
        client_root = PathlibPathEx(str(root))
    if not client_root.exists():
        client_root.mkdir(parents=True)
    _fs_client = BucketClientFS(root=client_root)
    return _fs_client


def get_fs_client() -> Optional[BucketClientFS]:
    """Get the file-system client (or None)"""
    global _fs_client
    assert _fs_client is None or isinstance(
        _fs_client, BucketClientFS
    ), "invalid root type"
    return _fs_client


def use_fs_cache(
    root: Optional[Union[str, PathBase, PathlibPathEx, bool]] = None
) -> Optional[PathlibPathEx]:
    """Use a path in the local file-system to cache blobs and buckets.

    This is useful for when you want to avoid fetching large blobs multiple
    times, or need to pass a local file path to a third-party library."""
    global _fs_cache
    # False - disable adapter
    if root is False:
        _fs_cache = None
        return None

    # None or True - enable FS cache with default root
    if root is None or root is True:
        # Use a temporary folder. Cache will be removed according to OS policy
        cache_root = PathlibPathEx(tempfile.mkdtemp())
    else:
        assert isinstance(
            root, (str, PathBase, PathlibPathEx, BasePathlibPath)
        ), f"root is not a known type: {type(root)}"
        cache_root = PathlibPathEx(str(root))
    if not cache_root.exists():
        cache_root.mkdir(parents=True)
    _fs_cache = cache_root
    return cache_root


def get_fs_cache() -> Optional[PathlibPathEx]:
    """Get the folder that holds file-system cached blobs and timestamps."""
    global _fs_cache
    assert _fs_cache is None or isinstance(
        _fs_cache, PathlibPathEx
    ), "invalid root type"
    return _fs_cache


def clear_fs_cache(force: bool = False) -> None:
    """Remove the existing file-system blob cache folder.

    Raises AssertionError if the cache path is unset or points to the
    root of the file-system."""
    cache_path = get_fs_cache()
    assert cache_path is not None, "no cache to clear"
    resolved = cache_path.resolve()
    assert str(resolved) != "/", f"refusing to remove a root path: {resolved}"
    shutil.rmtree(str(resolved))
