from __future__ import annotations

from abc import ABC, abstractmethod
from hashlib import md5
from inspect import isasyncgen, isgenerator
from io import BytesIO
from os import PathLike
from types import TracebackType
from typing import (
    Any,
    AnyStr,
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Optional,
    overload,
    Union,
)

from aiofiles import open as async_open
from aiofiles.base import AiofilesContextManager
from aiofiles.threadpool.binary import AsyncBufferedIOBase, AsyncBufferedReader
from werkzeug.datastructures import ContentRange, Headers, Range
from werkzeug.exceptions import RequestedRangeNotSatisfiable
from werkzeug.sansio.response import Response as SansIOResponse

from .. import json
from ..globals import current_app
from ..utils import file_path_to_path, run_sync_iterable

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type: ignore


class ResponseBody(ABC):
    """Base class wrapper for response body data.

    This ensures that the following is possible (as Quart assumes so
    when returning the body to the ASGI server

        async with wrapper as response:
            async for data in response:
                send(data)

    """

    @abstractmethod
    async def __aenter__(self) -> AsyncIterable:
        pass

    @abstractmethod
    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        pass


def _raise_if_invalid_range(begin: int, end: int, size: int) -> None:
    if begin >= end or abs(begin) > size or end > size:
        raise RequestedRangeNotSatisfiable()


class DataBody(ResponseBody):
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.begin = 0
        self.end = len(self.data)

    async def __aenter__(self) -> "DataBody":
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        pass

    def __aiter__(self) -> AsyncIterator:
        async def _aiter() -> AsyncGenerator[bytes, None]:
            yield self.data[self.begin : self.end]

        return _aiter()

    async def make_conditional(
        self, begin: int, end: Optional[int], max_partial_size: Optional[int] = None
    ) -> int:
        self.begin = begin
        self.end = len(self.data) if end is None else end
        if max_partial_size is not None:
            self.end = min(self.begin + max_partial_size, self.end)
        _raise_if_invalid_range(self.begin, self.end, len(self.data))
        return len(self.data)


class IterableBody(ResponseBody):
    def __init__(self, iterable: Union[AsyncGenerator[bytes, None], Iterable]) -> None:
        self.iter: AsyncGenerator[bytes, None]
        if isasyncgen(iterable):
            self.iter = iterable
        elif isgenerator(iterable):
            self.iter = run_sync_iterable(iterable)
        else:

            async def _aiter() -> AsyncGenerator[bytes, None]:
                for data in iterable:  # type: ignore
                    yield data

            self.iter = _aiter()

    async def __aenter__(self) -> "IterableBody":
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self.iter.aclose()

    def __aiter__(self) -> AsyncIterator:
        return self.iter


class FileBody(ResponseBody):
    """Provides an async file accessor with range setting.

    The :attr:`Response.response` attribute must be async-iterable and
    yield bytes, which this wrapper does for a file. In addition it
    allows a range to be set on the file, thereby supporting
    conditional requests.

    Attributes:
        buffer_size: Size in bytes to load per iteration.
    """

    buffer_size = 8192

    def __init__(
        self, file_path: Union[str, PathLike], *, buffer_size: Optional[int] = None
    ) -> None:
        self.file_path = file_path_to_path(file_path)
        self.size = self.file_path.stat().st_size
        self.begin = 0
        self.end = self.size
        if buffer_size is not None:
            self.buffer_size = buffer_size
        self.file: Optional[AsyncBufferedIOBase] = None
        self.file_manager: AiofilesContextManager[None, None, AsyncBufferedReader] = None

    async def __aenter__(self) -> "FileBody":
        self.file_manager = async_open(self.file_path, mode="rb")
        self.file = await self.file_manager.__aenter__()
        await self.file.seek(self.begin)
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        await self.file_manager.__aexit__(exc_type, exc_value, tb)

    def __aiter__(self) -> "FileBody":
        return self

    async def __anext__(self) -> bytes:
        current = await self.file.tell()
        if current >= self.end:
            raise StopAsyncIteration()
        read_size = min(self.buffer_size, self.end - current)
        chunk = await self.file.read(read_size)

        if chunk:
            return chunk
        else:
            raise StopAsyncIteration()

    async def make_conditional(
        self, begin: int, end: Optional[int], max_partial_size: Optional[int] = None
    ) -> int:
        self.begin = begin
        self.end = self.size if end is None else end
        if max_partial_size is not None:
            self.end = min(self.begin + max_partial_size, self.end)
        _raise_if_invalid_range(self.begin, self.end, self.size)
        return self.size


class IOBody(ResponseBody):
    """Provides an async file accessor with range setting.

    The :attr:`Response.response` attribute must be async-iterable and
    yield bytes, which this wrapper does for a file. In addition it
    allows a range to be set on the file, thereby supporting
    conditional requests.

    Attributes:
        buffer_size: Size in bytes to load per iteration.
    """

    buffer_size = 8192

    def __init__(self, io_stream: BytesIO, *, buffer_size: Optional[int] = None) -> None:
        self.io_stream = io_stream
        self.size = io_stream.getbuffer().nbytes
        self.begin = 0
        self.end = self.size
        if buffer_size is not None:
            self.buffer_size = buffer_size

    async def __aenter__(self) -> "IOBody":
        self.io_stream.seek(self.begin)
        return self

    async def __aexit__(self, exc_type: type, exc_value: BaseException, tb: TracebackType) -> None:
        return None

    def __aiter__(self) -> "IOBody":
        return self

    async def __anext__(self) -> bytes:
        current = self.io_stream.tell()
        if current >= self.end:
            raise StopAsyncIteration()
        read_size = min(self.buffer_size, self.end - current)
        chunk = self.io_stream.read(read_size)

        if chunk:
            return chunk
        else:
            raise StopAsyncIteration()

    async def make_conditional(
        self, begin: int, end: Optional[int], max_partial_size: Optional[int] = None
    ) -> int:
        self.begin = begin
        self.end = self.size if end is None else end
        if max_partial_size is not None:
            self.end = min(self.begin + max_partial_size, self.end)
        _raise_if_invalid_range(self.begin, self.end, self.size)
        return self.size


class Response(SansIOResponse):
    """This class represents a response.

    It can be subclassed and the subclassed used in preference by
    replacing the :attr:`~quart.Quart.response_class` with your
    subclass.

    Attributes:
        automatically_set_content_length: If False the content length
            header must be provided.
        default_status: The status code to use if not provided.
        default_mimetype: The mimetype to use if not provided.
        implicit_sequence_conversion: Implicitly convert the response
            to a iterable in the get_data method, to allow multiple
            iterations.
    """

    automatically_set_content_length = True
    default_mimetype = "text/html"
    data_body_class = DataBody
    file_body_class = FileBody
    implicit_sequence_conversion = True
    io_body_class = IOBody
    iterable_body_class = IterableBody
    json_module = json

    def __init__(
        self,
        response: Union[ResponseBody, AnyStr, Iterable],
        status: Optional[int] = None,
        headers: Optional[Union[dict, Headers]] = None,
        mimetype: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> None:
        """Create a response object.

        The response itself can be a chunk of data or a
        iterable/generator of data chunks.

        The Content-Type can either be specified as a mimetype or
        content_type header or omitted to use the
        :attr:`default_mimetype`.

        Arguments:
            response: The response data or iterable over the data.
            status: Status code of the response.
            headers: Headers to attach to the response.
            mimetype: Mimetype of the response.
            content_type: Content-Type header value.

        Attributes:
            response: An iterable of the response bytes-data.
        """
        super().__init__(status, headers, mimetype, content_type)
        self.timeout: Any = Ellipsis

        self.response: ResponseBody
        if isinstance(response, ResponseBody):
            self.response = response
        elif isinstance(response, (str, bytes)):
            self.set_data(response)  # type: ignore
        else:
            self.response = self.iterable_body_class(response)

    @property
    def max_cookie_size(self) -> int:  # type: ignore
        if current_app:
            return current_app.config["MAX_COOKIE_SIZE"]

        return super().max_cookie_size

    @overload
    async def get_data(self, as_text: Literal[True]) -> str:
        ...

    @overload
    async def get_data(self, as_text: Literal[False]) -> bytes:
        ...

    @overload
    async def get_data(self, as_text: bool = True) -> AnyStr:
        ...

    async def get_data(self, as_text: bool = False) -> AnyStr:
        """Return the body data."""
        if self.implicit_sequence_conversion:
            await self.make_sequence()
        result = "" if as_text else b""
        async with self.response as body:
            async for data in body:
                if as_text:
                    result += data.decode(self.charset)
                else:
                    result += data
        return result  # type: ignore

    def set_data(self, data: AnyStr) -> None:
        """Set the response data.

        This will encode using the :attr:`charset`.
        """
        if isinstance(data, str):
            bytes_data = data.encode(self.charset)
        else:
            bytes_data = data
        self.response = self.data_body_class(bytes_data)
        if self.automatically_set_content_length:
            self.content_length = len(bytes_data)

    @property
    async def data(self) -> bytes:
        return await self.get_data()

    @data.setter
    def data(self, value: bytes) -> None:
        self.set_data(value)

    @property
    async def json(self) -> Any:
        return await self.get_json()

    async def get_json(self, force: bool = False, silent: bool = False) -> Any:
        """Parses the body data as JSON and returns it.

        Arguments:
            force: Force JSON parsing even if the mimetype is not JSON.
            silent: Do not trigger error handling if parsing fails, without
                this the :meth:`on_json_loading_failed` will be called on
                error.
        """
        if not (force or self.is_json):
            return None

        data = await self.get_data(as_text=True)
        try:
            return self.json_module.loads(data)
        except ValueError:
            if silent:
                raise
            return None

    async def make_conditional(
        self, request_range: Optional[Range], max_partial_size: Optional[int] = None
    ) -> None:
        """Make the response conditional to the

        Arguments:
            request_range: The range as requested by the request.
            max_partial_size: The maximum length the server is willing
                to serve in a single response. Defaults to unlimited.

        """
        self.accept_ranges = "bytes"  # Advertise this ability
        if request_range is None or len(request_range.ranges) == 0:  # Not a conditional request
            return

        if request_range.units != "bytes" or len(request_range.ranges) > 1:
            raise RequestedRangeNotSatisfiable()

        begin, end = request_range.ranges[0]
        try:
            complete_length = await self.response.make_conditional(  # type: ignore
                begin, end, max_partial_size
            )
        except AttributeError:
            await self.make_sequence()
            return await self.make_conditional(request_range, max_partial_size)
        else:
            self.content_length = self.response.end - self.response.begin  # type: ignore
            if self.content_length != complete_length:
                self.content_range = ContentRange(
                    request_range.units,
                    self.response.begin,  # type: ignore
                    self.response.end - 1,  # type: ignore
                    complete_length,
                )
                self.status_code = 206

    async def make_sequence(self) -> None:
        data = b"".join([value async for value in self.iter_encode()])
        self.response = self.data_body_class(data)

    async def iter_encode(self) -> AsyncGenerator[bytes, None]:
        async with self.response as response_body:
            async for item in response_body:
                if isinstance(item, str):
                    yield item.encode(self.charset)
                else:
                    yield item

    async def freeze(self) -> None:
        """Freeze this object ready for pickling."""
        self.set_data((await self.get_data()))

    async def add_etag(self, overwrite: bool = False, weak: bool = False) -> None:
        if overwrite or "etag" not in self.headers:
            self.set_etag(md5((await self.get_data(as_text=False))).hexdigest(), weak)

    def _set_or_pop_header(self, key: str, value: str) -> None:
        if value == "":
            self.headers.pop(key, None)
        else:
            self.headers[key] = value

    @property
    def referrer(self) -> Optional[str]:
        return self.headers.get("Referer")

    @referrer.setter
    def referrer(self, value: str) -> None:
        self.headers["Referer"] = value
