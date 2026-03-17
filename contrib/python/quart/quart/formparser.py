from __future__ import annotations

from typing import (
    Any,
    Awaitable,
    Callable,
    cast,
    Dict,
    IO,
    List,
    NoReturn,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

from werkzeug.datastructures import Headers, MultiDict
from werkzeug.formparser import default_stream_factory
from werkzeug.http import parse_options_header
from werkzeug.sansio.multipart import Data, Epilogue, Field, File, MultipartDecoder, NeedData
from werkzeug.urls import url_decode

from .datastructures import FileStorage

if TYPE_CHECKING:
    from .wrappers.request import Body

StreamFactory = Callable[
    [Optional[int], Optional[str], Optional[str], Optional[int]],
    IO[bytes],
]

ParserFunc = Callable[
    ["FormDataParser", "Body", str, Optional[int], Dict[str, str]],
    Awaitable[Tuple[MultiDict, MultiDict]],
]


class FormDataParser:
    file_storage_class = FileStorage

    def __init__(
        self,
        stream_factory: StreamFactory = default_stream_factory,
        charset: str = "utf-8",
        errors: str = "replace",
        max_form_memory_size: Optional[int] = None,
        max_content_length: Optional[int] = None,
        cls: Optional[Type[MultiDict]] = MultiDict,
        silent: bool = True,
    ) -> None:
        self.stream_factory = stream_factory
        self.charset = charset
        self.errors = errors
        self.cls = cls
        self.silent = silent

    def get_parse_func(self, mimetype: str, options: Dict[str, str]) -> Optional[ParserFunc]:
        return self.parse_functions.get(mimetype)

    async def parse(
        self,
        body: "Body",
        mimetype: str,
        content_length: Optional[int],
        options: Optional[Dict[str, str]] = None,
    ) -> Tuple[MultiDict, MultiDict]:
        if options is None:
            options = {}

        parse_func = self.get_parse_func(mimetype, options)

        if parse_func is not None:
            try:
                return await parse_func(self, body, mimetype, content_length, options)
            except ValueError:
                if not self.silent:
                    raise

        return self.cls(), self.cls()

    async def _parse_multipart(
        self,
        body: "Body",
        mimetype: str,
        content_length: Optional[int],
        options: Dict[str, str],
    ) -> Tuple[MultiDict, MultiDict]:
        parser = MultiPartParser(
            self.stream_factory,
            self.charset,
            self.errors,
            cls=self.cls,
            file_storage_cls=self.file_storage_class,
        )
        boundary = options.get("boundary", "").encode("ascii")

        if not boundary:
            raise ValueError("Missing boundary")

        return await parser.parse(body, boundary, content_length)

    async def _parse_urlencoded(
        self,
        body: "Body",
        mimetype: str,
        content_length: Optional[int],
        options: Dict[str, str],
    ) -> Tuple[MultiDict, MultiDict]:
        form = url_decode(await body, self.charset, errors=self.errors, cls=self.cls)
        return form, self.cls()

    parse_functions: Dict[str, ParserFunc] = {
        "multipart/form-data": _parse_multipart,
        "application/x-www-form-urlencoded": _parse_urlencoded,
        "application/x-url-encoded": _parse_urlencoded,
    }


class MultiPartParser:
    def __init__(
        self,
        stream_factory: StreamFactory = default_stream_factory,
        charset: str = "utf-8",
        errors: str = "replace",
        max_form_memory_size: Optional[int] = None,
        cls: Type[MultiDict] = MultiDict,
        buffer_size: int = 64 * 1024,
        file_storage_cls: Type[FileStorage] = FileStorage,
    ) -> None:
        self.charset = charset
        self.errors = errors
        self.max_form_memory_size = max_form_memory_size
        self.stream_factory = stream_factory
        self.cls = cls
        self.buffer_size = buffer_size
        self.file_storage_cls = file_storage_cls

    def fail(self, message: str) -> NoReturn:
        raise ValueError(message)

    def get_part_charset(self, headers: Headers) -> str:
        content_type = headers.get("content-type")

        if content_type:
            mimetype, ct_params = parse_options_header(content_type)
            return ct_params.get("charset", self.charset)

        return self.charset

    def start_file_streaming(self, event: File, total_content_length: int) -> IO[bytes]:
        content_type = event.headers.get("content-type")

        try:
            content_length = int(event.headers["content-length"])
        except (KeyError, ValueError):
            content_length = 0

        container = self.stream_factory(
            total_content_length,
            content_type,
            event.filename,
            content_length,
        )
        return container

    async def parse(
        self, body: "Body", boundary: bytes, content_length: int
    ) -> Tuple[MultiDict, MultiDict]:
        container: Union[IO[bytes], List[bytes]]
        _write: Callable[[bytes], Any]

        parser = MultipartDecoder(boundary, self.max_form_memory_size)

        fields = []
        files = []

        current_part: Union[Field, File]
        async for data in body:
            parser.receive_data(data)
            event = parser.next_event()
            while not isinstance(event, (Epilogue, NeedData)):
                if isinstance(event, Field):
                    current_part = event
                    container = []
                    _write = container.append
                elif isinstance(event, File):
                    current_part = event
                    container = self.start_file_streaming(event, content_length)
                    _write = container.write
                elif isinstance(event, Data):
                    _write(event.data)
                    if not event.more_data:
                        if isinstance(current_part, Field):
                            value = b"".join(container).decode(
                                self.get_part_charset(current_part.headers), self.errors
                            )
                            fields.append((current_part.name, value))
                        else:
                            container = cast(IO[bytes], container)
                            container.seek(0)
                            files.append(
                                (
                                    current_part.name,
                                    self.file_storage_cls(
                                        container,
                                        current_part.filename,
                                        current_part.name,
                                        headers=current_part.headers,
                                    ),
                                )
                            )

                event = parser.next_event()

        return self.cls(fields), self.cls(files)
