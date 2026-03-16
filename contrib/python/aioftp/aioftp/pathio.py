import abc
import asyncio
import functools
import io
import operator
import stat
import sys
import time
from collections.abc import AsyncIterable, Awaitable, Callable, Generator, Iterator
from concurrent.futures import Executor
from os import stat_result
from pathlib import Path, PurePath, PurePosixPath
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    NamedTuple,
    Protocol,
    TypedDict,
    TypeVar,
    overload,
)

from .common import (
    DEFAULT_BLOCK_SIZE,
    AbstractAsyncLister,
    AsyncStreamIterator,
    Connection,
    with_timeout,
)
from .errors import PathIOError

if sys.version_info >= (3, 11):
    from typing import Concatenate, ParamSpec, Self, Unpack
else:
    from typing import Concatenate

    from typing_extensions import ParamSpec, Self, Unpack


if TYPE_CHECKING:
    from _typeshed import OpenBinaryMode, ReadableBuffer


__all__ = (
    "AbstractPathIO",
    "PathIO",
    "AsyncPathIO",
    "MemoryPathIO",
    "PathIONursery",
)


PathIOType = TypeVar("PathIOType", bound="AbstractPathIO[Any]")


class _OpenKwargs(TypedDict, total=False):
    path: Path
    mode: str


class AsyncPathIOContext:
    """
    Async pathio context.

    Usage:
    ::

        >>> async with pathio.open(filename) as file_in:
        ...     async for block in file_in.iter_by_block(size):
        ...         # do

    or borring:
    ::

        >>> file = await pathio.open(filename)
        ... data = await file.read(size)
        ... await file.write(data)
        ... await file.close()

    """

    def __init__(self, pathio: PathIOType, args: tuple[Any], kwargs: _OpenKwargs) -> None:
        self.close: Callable[..., Awaitable[None]] | None = None
        self.pathio = pathio
        self.args = args
        self.kwargs = kwargs

    async def __aenter__(self) -> Self:
        self.file = await self.pathio._open(*self.args, **self.kwargs)
        self.seek = functools.partial(self.pathio.seek, self.file)
        self.write = functools.partial(self.pathio.write, self.file)
        self.read = functools.partial(self.pathio.read, self.file)
        self.close = functools.partial(self.pathio.close, self.file)

        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.close is not None:
            await self.close()

    def __await__(self) -> Generator[None, None, Self]:
        return self.__aenter__().__await__()

    def iter_by_block(self, count: int = DEFAULT_BLOCK_SIZE) -> AsyncIterable[bytes]:
        return AsyncStreamIterator(lambda: self.read(count))


UniversalExceptionParamSpec = ParamSpec("UniversalExceptionParamSpec")
UniversalExceptionReturnType = TypeVar("UniversalExceptionReturnType")


def universal_exception(
    coro: Callable[UniversalExceptionParamSpec, Awaitable[UniversalExceptionReturnType]],
) -> Callable[UniversalExceptionParamSpec, Awaitable[UniversalExceptionReturnType]]:
    """
    Decorator. Reraising any exception (except `CancelledError` and
    `NotImplementedError`) with universal exception
    :py:class:`aioftp.PathIOError`
    """

    @functools.wraps(coro)
    async def wrapper(
        *args: UniversalExceptionParamSpec.args,
        **kwargs: UniversalExceptionParamSpec.kwargs,
    ) -> UniversalExceptionReturnType:
        try:
            return await coro(*args, **kwargs)
        except (
            asyncio.CancelledError,
            NotImplementedError,
            StopAsyncIteration,
        ):
            raise
        except Exception as exc:
            raise PathIOError(reason=sys.exc_info()) from exc

    return wrapper


class _DirNodeProtocol(Protocol):
    type: Literal["dir"]
    name: str
    ctime: int
    mtime: int
    content: list["NodeProtocol"]


class _FileNodeProtocol(Protocol):
    type: Literal["file"]
    name: str
    ctime: int
    mtime: int
    content: io.BytesIO


NodeProtocol = _DirNodeProtocol | _FileNodeProtocol


class PathIONursery(Generic[PathIOType]):
    def __init__(self, factory: type["PathIOType"]) -> None:
        self.factory = factory
        self.state: list[Node] | None = None

    def __call__(
        self,
        timeout: float | int | None = None,
        connection: Connection | None = None,
        state: list["Node"] | None = None,
    ) -> PathIOType:
        instance = self.factory(timeout=timeout, connection=connection, state=self.state)
        if self.state is None:
            self.state = instance.state
        return instance


DefendFileMethodsParamSpec = ParamSpec("DefendFileMethodsParamSpec")
DefendFileMethodsReturnType = TypeVar("DefendFileMethodsReturnType")


def defend_file_methods(
    coro: Callable[
        DefendFileMethodsParamSpec,
        Awaitable[DefendFileMethodsReturnType],
    ],
) -> Callable[
    DefendFileMethodsParamSpec,
    Awaitable[DefendFileMethodsReturnType],
]:
    """
    Decorator. Raises exception when file methods called with wrapped by
    :py:class:`aioftp.AsyncPathIOContext` file object.
    """

    @functools.wraps(coro)
    async def wrapper(
        *args: DefendFileMethodsParamSpec.args,
        **kwargs: DefendFileMethodsParamSpec.kwargs,
    ) -> DefendFileMethodsReturnType:
        file = args[1]

        if isinstance(file, AsyncPathIOContext):
            raise ValueError(
                "Native path io file methods can not be used with wrapped file object",
            )
        return await coro(*args, **kwargs)

    return wrapper


class PathlibOpenKwargs(TypedDict, total=False):
    mode: "OpenBinaryMode"
    buffering: int
    encoding: str | None
    errors: str | None
    newline: str | None


PathType = TypeVar("PathType", bound=PurePath)


class AbstractPathIO(Generic[PathType], abc.ABC):
    """
    Abstract class for path io operations.

    :param timeout: timeout used by `with_timeout` decorator
    :type timeout: :py:class:`float`, :py:class:`int` or `None`

    :param connection: server connection that is accessing this PathIO
    :type connection: :py:class:`aioftp.Connection`

    :param state: shared pathio state per server
    """

    def __init__(
        self,
        timeout: float | int | None = None,
        connection: Connection | None = None,
        state: list["Node"] | None = None,
    ) -> None:
        self.timeout = timeout
        self.connection = connection

    @property
    def state(self) -> list["Node"] | None:
        """
        Shared pathio state per server
        """

    @universal_exception
    @abc.abstractmethod
    async def exists(self, path: PathType) -> bool:
        """
        :py:func:`asyncio.coroutine`

        Check if path exists

        :param path: path to check
        :type path: :py:class:`pathlib.Path`

        :rtype: :py:class:`bool`
        """

    @universal_exception
    @abc.abstractmethod
    async def is_dir(self, path: PathType) -> bool:
        """
        :py:func:`asyncio.coroutine`

        Check if path is directory

        :param path: path to check
        :type path: :py:class:`pathlib.Path`

        :rtype: :py:class:`bool`
        """

    @universal_exception
    @abc.abstractmethod
    async def is_file(self, path: PathType) -> bool:
        """
        :py:func:`asyncio.coroutine`

        Check if path is file

        :param path: path to check
        :type path: :py:class:`pathlib.Path`

        :rtype: :py:class:`bool`
        """

    @universal_exception
    @abc.abstractmethod
    async def mkdir(self, path: PathType, *, parents: bool = False, exist_ok: bool = False) -> None:
        """
        :py:func:`asyncio.coroutine`

        Make directory

        :param path: path to create
        :type path: :py:class:`pathlib.Path`

        :param parents: create parents is does not exists
        :type parents: :py:class:`bool`

        :param exist_ok: do not raise exception if directory already exists
        :type exist_ok: :py:class:`bool`
        """

    @universal_exception
    @abc.abstractmethod
    async def rmdir(self, path: PathType) -> None:
        """
        :py:func:`asyncio.coroutine`

        Remove directory

        :param path: path to remove
        :type path: :py:class:`pathlib.Path`
        """

    @universal_exception
    @abc.abstractmethod
    async def unlink(self, path: PathType) -> None:
        """
        :py:func:`asyncio.coroutine`

        Remove file

        :param path: path to remove
        :type path: :py:class:`pathlib.Path`
        """

    @abc.abstractmethod
    def list(self, path: PathType) -> AsyncIterable[PathType]:
        """
        Create instance of subclass of :py:class:`aioftp.AbstractAsyncLister`.
        You should subclass and implement `__anext__` method
        for :py:class:`aioftp.AbstractAsyncLister` and return new instance.

        :param path: path to list
        :type path: :py:class:`pathlib.Path`

        :rtype: :py:class:`aioftp.AbstractAsyncLister`

        Usage:
        ::

            >>> async for p in pathio.list(path):
            ...     # do

        or borring instance of :py:class:`list`:
        ::

            >>> paths = await pathio.list(path)
            >>> paths
            [path, path, path, ...]

        """

    @universal_exception
    @abc.abstractmethod
    async def stat(self, path: PathType) -> stat_result:
        """
        :py:func:`asyncio.coroutine`

        Get path stats

        :param path: path, which stats need
        :type path: :py:class:`pathlib.Path`

        :return: path stats. For proper work you need only this stats:
          st_size, st_mtime, st_ctime, st_nlink, st_mode
        :rtype: same as :py:class:`os.stat_result`
        """

    @universal_exception
    @abc.abstractmethod
    async def _open(self, path: PathType, mode: str) -> io.BytesIO:
        """
        :py:func:`asyncio.coroutine`

        Open file. You should implement "mode" argument, which can be:
        "rb", "wb", "ab" (read, write, append. all binary). Return type depends
        on implementation, anyway the only place you need this file-object
        is in your implementation of read, write and close

        :param path: path to create
        :type path: :py:class:`pathlib.Path`

        :param mode: specifies the mode in which the file is opened ("rb",
            "wb", "ab", "r+b" (read, write, append, read/write, all binary))
        :type mode: :py:class:`str`

        :return: file-object
        """

    def open(self, *args: Unpack[tuple[PathType]], **kwargs: Unpack[_OpenKwargs]) -> AsyncPathIOContext:
        """
        Create instance of :py:class:`aioftp.pathio.AsyncPathIOContext`,
        parameters passed to :py:meth:`aioftp.AbstractPathIO._open`

        :rtype: :py:class:`aioftp.pathio.AsyncPathIOContext`
        """
        return AsyncPathIOContext(self, args, kwargs)

    @universal_exception
    @defend_file_methods
    @abc.abstractmethod
    async def seek(self, file: io.BytesIO, offset: int, whence: int = io.SEEK_SET) -> int:
        """
        :py:func:`asyncio.coroutine`

        Change the stream position to the given byte `offset`. Same behaviour
        as :py:meth:`io.IOBase.seek`

        :param file: file-object from :py:class:`aioftp.AbstractPathIO.open`

        :param offset: relative byte offset
        :type offset: :py:class:`int`

        :param whence: base position for offset
        :type whence: :py:class:`int`
        """

    @universal_exception
    @defend_file_methods
    @abc.abstractmethod
    async def write(self, file: io.BytesIO, data: bytes) -> int:
        """
        :py:func:`asyncio.coroutine`

        Write some data to file

        :param file: file-object from :py:class:`aioftp.AbstractPathIO.open`

        :param data: data to write
        :type data: :py:class:`bytes`
        """

    @universal_exception
    @defend_file_methods
    @abc.abstractmethod
    async def read(self, file: io.BytesIO, block_size: int) -> bytes:
        """
        :py:func:`asyncio.coroutine`

        Read some data from file

        :param file: file-object from :py:class:`aioftp.AbstractPathIO.open`

        :param block_size: bytes count to read
        :type block_size: :py:class:`int`

        :rtype: :py:class:`bytes`
        """

    @universal_exception
    @defend_file_methods
    @abc.abstractmethod
    async def close(self, file: io.BytesIO) -> None:
        """
        :py:func:`asyncio.coroutine`

        Close file

        :param file: file-object from :py:class:`aioftp.AbstractPathIO.open`
        """

    @universal_exception
    @abc.abstractmethod
    async def rename(self, source: PathType, destination: PathType) -> PathType:
        """
        :py:func:`asyncio.coroutine`

        Rename path

        :param source: rename from
        :type source: :py:class:`pathlib.Path`

        :param destination: rename to
        :type destination: :py:class:`pathlib.Path`
        """


class PathIO(AbstractPathIO[Path]):
    """
    Blocking path io. Directly based on :py:class:`pathlib.Path` methods.
    """

    @universal_exception
    async def exists(self, path: Path) -> bool:
        return path.exists()

    @universal_exception
    async def is_dir(self, path: Path) -> bool:
        return path.is_dir()

    @universal_exception
    async def is_file(self, path: Path) -> bool:
        return path.is_file()

    @universal_exception
    async def mkdir(self, path: Path, *, parents: bool = False, exist_ok: bool = False) -> None:
        return path.mkdir(parents=parents, exist_ok=exist_ok)

    @universal_exception
    async def rmdir(self, path: Path) -> None:
        return path.rmdir()

    @universal_exception
    async def unlink(self, path: Path) -> None:
        return path.unlink()

    def list(self, path: Path) -> AsyncIterable[Path]:
        class Lister(AbstractAsyncLister[Path]):
            iter: Iterator[Path] | None = None

            @universal_exception
            async def __anext__(self) -> Path:
                if self.iter is None:
                    self.iter = path.glob("*")
                try:
                    return next(self.iter)
                except StopIteration:
                    raise StopAsyncIteration

        return Lister(timeout=self.timeout)

    @universal_exception
    async def stat(self, path: Path) -> stat_result:
        return path.stat()

    @universal_exception
    async def _open(  # type: ignore[override]
        self,
        path: Path,
        mode: "OpenBinaryMode" = "rb",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> io.BytesIO:
        return path.open(  # type: ignore[return-value]
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    @universal_exception
    @defend_file_methods
    async def seek(
        self,
        file: io.BytesIO,
        offset: int,
        whence: int = io.SEEK_SET,
    ) -> int:
        return file.seek(offset, whence)

    @universal_exception
    @defend_file_methods
    async def write(self, file: io.BytesIO, s: "ReadableBuffer") -> int:
        return file.write(s)

    @universal_exception
    @defend_file_methods
    async def read(self, file: io.BytesIO, n: int = -1) -> bytes:
        return file.read(n)

    @universal_exception
    @defend_file_methods
    async def close(self, file: io.BytesIO) -> None:
        return file.close()

    @universal_exception
    async def rename(self, source: Path, destination: Path) -> Path:
        return source.rename(destination)


class HasExecutor(Protocol):
    executor: Executor | None


_BlockingIOType = TypeVar("_BlockingIOType", bound=HasExecutor)
_BlockingIOParamSpec = ParamSpec("_BlockingIOParamSpec")
_BlockingIOReturnType = TypeVar("_BlockingIOReturnType")


def _blocking_io(
    f: Callable[Concatenate[_BlockingIOType, _BlockingIOParamSpec], _BlockingIOReturnType],
) -> Callable[Concatenate[_BlockingIOType, _BlockingIOParamSpec], Awaitable[_BlockingIOReturnType]]:
    @functools.wraps(f)
    async def wrapper(
        self: _BlockingIOType,
        *args: _BlockingIOParamSpec.args,
        **kwargs: _BlockingIOParamSpec.kwargs,
    ) -> _BlockingIOReturnType:
        return await asyncio.get_running_loop().run_in_executor(
            self.executor,
            functools.partial(f, self, *args, **kwargs),
        )

    # Concatenate doesn't support pos-only arguments
    return wrapper  # type: ignore[return-value]


class AsyncPathIO(AbstractPathIO[Path]):
    """
    Non-blocking path io. Based on
    :py:meth:`asyncio.BaseEventLoop.run_in_executor` and
    :py:class:`pathlib.Path` methods. It's really slow, so it's better to avoid
    usage of this path io layer.

    :param executor: executor for running blocking tasks
    :type executor: :py:class:`concurrent.futures.Executor`
    """

    def __init__(
        self,
        timeout: float | int | None = None,
        connection: Connection | None = None,
        state: list["Node"] | None = None,
        executor: Executor | None = None,
    ) -> None:
        super().__init__(timeout, connection, state)
        self.executor = executor

    @universal_exception
    @with_timeout
    @_blocking_io
    def exists(self, path: Path) -> bool:
        return path.exists()

    @universal_exception
    @with_timeout
    @_blocking_io
    def is_dir(self, path: Path) -> bool:
        return path.is_dir()

    @universal_exception
    @with_timeout
    @_blocking_io
    def is_file(self, path: Path) -> bool:
        return path.is_file()

    @universal_exception
    @with_timeout
    @_blocking_io
    def mkdir(self, path: Path, *, parents: bool = False, exist_ok: bool = False) -> None:
        return path.mkdir(parents=parents, exist_ok=exist_ok)

    @universal_exception
    @with_timeout
    @_blocking_io
    def rmdir(self, path: Path) -> None:
        return path.rmdir()

    @universal_exception
    @with_timeout
    @_blocking_io
    def unlink(self, path: Path) -> None:
        return path.unlink()

    def list(self, path: Path) -> AsyncIterable[Path]:
        class Lister(AbstractAsyncLister[Path]):
            def __init__(self, timeout: float | int | None = None, executor: Executor | None = None) -> None:
                super().__init__(timeout=timeout)
                self.executor = executor
                self.iter: Iterator[Path] | None = None

            @universal_exception
            @with_timeout
            @_blocking_io
            def __anext__(self) -> Path:
                if self.iter is None:
                    self.iter = path.glob("*")
                try:
                    return next(self.iter)
                except StopIteration:
                    raise StopAsyncIteration

        return Lister(timeout=self.timeout, executor=self.executor)

    @universal_exception
    @with_timeout
    @_blocking_io
    def stat(self, path: Path) -> stat_result:
        return path.stat()

    @universal_exception
    @with_timeout
    @_blocking_io
    def _open(  # type: ignore[override]
        self,
        path: Path,
        mode: "OpenBinaryMode" = "rb",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> io.BytesIO:
        return path.open(  # type: ignore[return-value]
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    @universal_exception
    @defend_file_methods
    @with_timeout
    @_blocking_io
    def seek(
        self,
        file: io.BytesIO,
        offset: int,
        whence: int = io.SEEK_SET,
    ) -> int:
        return file.seek(offset, whence)

    @universal_exception
    @defend_file_methods
    @with_timeout
    @_blocking_io
    def write(self, file: io.BytesIO, s: "ReadableBuffer") -> int:
        return file.write(s)

    @universal_exception
    @defend_file_methods
    @with_timeout
    @_blocking_io
    def read(self, file: io.BytesIO, n: int = -1) -> bytes:
        return file.read(n)

    @universal_exception
    @defend_file_methods
    @with_timeout
    @_blocking_io
    def close(self, file: io.BytesIO) -> None:
        return file.close()

    @universal_exception
    @with_timeout
    @_blocking_io
    def rename(self, source: Path, destination: Path) -> Path:
        return source.rename(destination)


class Node:
    @overload
    def __init__(
        self: _FileNodeProtocol,
        type: Literal["file"],
        name: str,
        ctime: int | None = None,
        mtime: int | None = None,
        *,
        content: io.BytesIO,
    ) -> None: ...

    @overload
    def __init__(
        self: _DirNodeProtocol,
        type: Literal["dir"],
        name: str,
        ctime: int | None = None,
        mtime: int | None = None,
        *,
        content: list["Node"],
    ) -> None: ...

    def __init__(
        self,
        type: Literal["dir", "file"],
        name: str,
        ctime: int | None = None,
        mtime: int | None = None,
        *,
        content: list["Node"] | io.BytesIO,
    ) -> None:
        self.type = type
        self.name = name
        self.ctime = ctime or int(time.time())
        self.mtime = mtime or int(time.time())
        self.content = content

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(type={self.type!r}, "
            f"name={self.name!r}, ctime={self.ctime!r}, "
            f"mtime={self.mtime!r}, content={self.content!r})"
        )


class MemoryPathIO(AbstractPathIO[PurePosixPath]):
    """
    Non-blocking path io. Based on in-memory tree. It is just proof of concept
    and probably not so fast as it can be.
    """

    class Stats(NamedTuple):
        st_size: int
        st_ctime: int
        st_mtime: int
        st_nlink: int
        st_mode: int

    def __init__(
        self,
        timeout: float | int | None = None,
        connection: Connection | None = None,
        state: list[Node] | None = None,
        cwd: str | PurePosixPath | None = None,
    ) -> None:
        super().__init__(timeout=timeout, connection=connection)
        self.cwd = PurePosixPath(cwd or "/")
        if state is None:
            self.fs = [Node("dir", "/", content=[])]
        else:
            self.fs = state

    @property
    def state(self) -> list[Node]:
        return self.fs

    def __repr__(self) -> str:
        return repr(self.fs)

    def _absolute(self, path: PurePosixPath) -> PurePosixPath:
        if not path.is_absolute():
            path = self.cwd / path
        return path

    def get_node(self, path: PurePosixPath) -> Node | None:
        nodes: list[Node] | io.BytesIO = self.fs
        node = None
        path = self._absolute(path)
        for part in path.parts:
            if not isinstance(nodes, list):
                return None
            for node in nodes:
                if node.name == part:
                    nodes = node.content
                    break
            else:
                return None
        return node

    @universal_exception
    async def exists(self, path: PurePosixPath) -> bool:
        return self.get_node(path) is not None

    @universal_exception
    async def is_dir(self, path: PurePosixPath) -> bool:
        node = self.get_node(path)
        return not (node is None or node.type != "dir")

    @universal_exception
    async def is_file(self, path: PurePosixPath) -> bool:
        node = self.get_node(path)
        return not (node is None or node.type != "file")

    @universal_exception
    async def mkdir(self, path: PurePosixPath, *, parents: bool = False, exist_ok: bool = False) -> None:
        path_ = self._absolute(path)
        node = self.get_node(path_)
        if node:
            if node.type != "dir" or not exist_ok:
                raise FileExistsError
        elif not parents:
            parent = self.get_node(path_.parent)
            if parent is None:
                raise FileNotFoundError
            if isinstance(parent.content, io.BytesIO):
                raise NotADirectoryError
            node = Node("dir", path_.name, content=[])
            parent.content.append(node)
        else:
            nodes: list[Node] | io.BytesIO = self.fs
            for part in path_.parts:
                if isinstance(nodes, list):
                    for node in nodes:
                        if node.name == part:
                            nodes = node.content
                            break
                    else:
                        node = Node("dir", part, content=[])
                        nodes.append(node)
                        nodes = node.content
                else:
                    raise NotADirectoryError

    @universal_exception
    async def rmdir(self, path: PurePosixPath) -> None:
        node = self.get_node(path)
        if node is None:
            raise FileNotFoundError
        if node.type != "dir":
            raise NotADirectoryError
        if node.content:
            raise OSError("Directory not empty")

        parent = self.get_node(path.parent)
        if parent is None:
            raise FileNotFoundError
        if not isinstance(parent.content, list):
            raise NotADirectoryError
        for i, node in enumerate(parent.content):
            if node.name == path.name:
                break
        parent.content.pop(i)

    @universal_exception
    async def unlink(self, path: PurePosixPath) -> None:
        node = self.get_node(path)
        if node is None:
            raise FileNotFoundError
        if node.type != "file":
            raise IsADirectoryError

        parent = self.get_node(path.parent)
        if parent is None:
            raise FileNotFoundError
        if not isinstance(parent.content, list):
            raise NotADirectoryError
        for i, node in enumerate(parent.content):
            if node.name == path.name:
                break
        parent.content.pop(i)

    def list(self, path: PurePosixPath) -> AsyncIterable[PurePosixPath]:
        class Lister(AbstractAsyncLister[PurePosixPath]):
            iter: Iterator[PurePosixPath] | None = None

            @universal_exception
            async def __anext__(cls) -> PurePosixPath:
                if cls.iter is None:
                    node = self.get_node(path)
                    if node is None or node.type != "dir":
                        cls.iter = iter(())
                    else:
                        names = map(operator.attrgetter("name"), node.content)
                        paths = map(lambda name: path / name, names)
                        cls.iter = iter(paths)
                try:
                    return next(cls.iter)
                except StopIteration:
                    raise StopAsyncIteration

        return Lister(timeout=self.timeout)

    @universal_exception
    async def stat(self, path: PurePosixPath) -> "MemoryPathIO.Stats":  # type: ignore[override]
        node = self.get_node(path)
        if node is None:
            raise FileNotFoundError

        if isinstance(node.content, io.BytesIO):
            size = len(node.content.getbuffer())
            mode = stat.S_IFREG | 0o666
        else:
            size = 0
            mode = stat.S_IFDIR | 0o777
        return MemoryPathIO.Stats(
            size,
            node.ctime,
            node.mtime,
            1,
            mode,
        )

    @universal_exception
    async def _open(  # type: ignore[override]
        self,
        path: PurePosixPath,
        mode: "OpenBinaryMode" = "rb",
    ) -> io.BytesIO:
        if mode == "rb":
            node = self.get_node(path)
            if node is None:
                raise FileNotFoundError
            if isinstance(node.content, list):
                raise IsADirectoryError
            file_like: io.BytesIO = node.content
            file_like.seek(0, io.SEEK_SET)
        elif mode in ("wb", "ab", "r+b"):
            node = self.get_node(path)
            if node is None:
                parent = self.get_node(path.parent)
                if parent is None or not isinstance(parent.content, list):
                    raise FileNotFoundError
                content = io.BytesIO()
                new_node = Node("file", path.name, content=content)
                parent.content.append(new_node)
                file_like = content
            elif node.type != "file":
                raise IsADirectoryError
            else:
                if isinstance(node.content, list):
                    raise IsADirectoryError
                if mode == "wb":
                    file_like = node.content = io.BytesIO()
                elif mode == "ab":
                    file_like = node.content
                    file_like.seek(0, io.SEEK_END)
                elif mode == "r+b":
                    file_like = node.content
                    file_like.seek(0, io.SEEK_SET)
        else:
            raise ValueError(f"invalid mode: {mode}")
        return file_like

    @universal_exception
    @defend_file_methods
    async def seek(self, file: io.BytesIO, offset: int, whence: int = io.SEEK_SET) -> int:
        return file.seek(offset, whence)

    @universal_exception
    @defend_file_methods
    async def write(self, file: io.BytesIO, buffer: "ReadableBuffer") -> int:
        x = file.write(buffer)
        file.mtime = int(time.time())  # type: ignore[attr-defined]
        return x

    @universal_exception
    @defend_file_methods
    async def read(self, file: io.BytesIO, size: int | None = -1) -> bytes:
        return file.read(size)

    @universal_exception
    @defend_file_methods
    async def close(self, file: io.BytesIO) -> None:
        pass

    @universal_exception
    async def rename(self, source: PurePosixPath, destination: PurePosixPath) -> PurePosixPath:
        if source != destination:
            sparent = self.get_node(source.parent)
            dparent = self.get_node(destination.parent)
            snode = self.get_node(source)
            if snode is None:
                raise FileNotFoundError
            if dparent is None:
                raise FileNotFoundError
            if isinstance(dparent.content, io.BytesIO):
                raise NotADirectoryError
            if sparent is None:
                raise FileNotFoundError
            if isinstance(sparent.content, io.BytesIO):
                raise NotADirectoryError
            for i, node in enumerate(sparent.content):
                if node.name == source.name:
                    sparent.content.pop(i)
            snode.name = destination.name
            for i, node in enumerate(dparent.content):
                if node.name == destination.name:
                    dparent.content[i] = snode
                    break
            else:
                dparent.content.append(snode)
        return destination
