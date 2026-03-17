from __future__ import annotations

import asyncio
from typing import (
    Any,
    AnyStr,
    Awaitable,
    Callable,
    Dict,
    Generator,
    List,
    NoReturn,
    Optional,
    overload,
)

from hypercorn.typing import HTTPScope
from werkzeug.datastructures import CombinedMultiDict, Headers, MultiDict
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge, RequestTimeout

from .base import BaseRequestWebsocket
from .. import json
from ..formparser import FormDataParser
from ..globals import current_app

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal  # type: ignore

SERVER_PUSH_HEADERS_TO_COPY = {
    "accept",
    "accept-encoding",
    "accept-language",
    "cache-control",
    "user-agent",
}


class Body:
    """A request body container.

    The request body can either be iterated over and consumed in parts
    (without building up memory usage) or awaited.

    .. code-block:: python

        async for data in body:
            ...
        # or simply
        complete = await body

    Note: It is not possible to iterate over the data and then await
    it.
    """

    def __init__(
        self, expected_content_length: Optional[int], max_content_length: Optional[int]
    ) -> None:
        self._data = bytearray()
        self._complete: asyncio.Event = asyncio.Event()
        self._has_data: asyncio.Event = asyncio.Event()
        self._max_content_length = max_content_length
        # Exceptions must be raised within application (not ASGI)
        # calls, this is achieved by having the ASGI methods set this
        # to an exception on error.
        self._must_raise: Optional[Exception] = None
        if (
            expected_content_length is not None
            and max_content_length is not None
            and expected_content_length > max_content_length
        ):
            self._must_raise = RequestEntityTooLarge()

    def __aiter__(self) -> "Body":
        return self

    async def __anext__(self) -> bytes:
        if self._must_raise is not None:
            raise self._must_raise

        # if we got all of the data in the first shot, then self._complete is
        # set and self._has_data will not get set again, so skip the await
        # if we already have completed everything
        if not self._complete.is_set():
            await self._has_data.wait()

        if self._complete.is_set() and len(self._data) == 0:
            raise StopAsyncIteration()

        data = bytes(self._data)
        self._data.clear()
        self._has_data.clear()
        return data

    def __await__(self) -> Generator[Any, None, Any]:
        # Must check the _must_raise before and after waiting on the
        # completion event as it may change whilst waiting and the
        # event may not be set if there is already an issue.
        if self._must_raise is not None:
            raise self._must_raise

        yield from self._complete.wait().__await__()

        if self._must_raise is not None:
            raise self._must_raise
        return bytes(self._data)

    def append(self, data: bytes) -> None:
        if data == b"" or self._must_raise is not None:
            return
        self._data.extend(data)
        self._has_data.set()
        if self._max_content_length is not None and len(self._data) > self._max_content_length:
            self._must_raise = RequestEntityTooLarge()
            self.set_complete()

    def set_complete(self) -> None:
        self._complete.set()
        self._has_data.set()

    def set_result(self, data: bytes) -> None:
        """Convenience method, mainly for testing."""
        self.append(data)
        self.set_complete()

    def clear(self) -> None:
        self._data.clear()


class Request(BaseRequestWebsocket):
    """This class represents a request.

    It can be subclassed and the subclassed used in preference by
    replacing the :attr:`~quart.Quart.request_class` with your
    subclass.

    Attributes:
        body_class: The class to store the body data within.
        form_data_parser_class: Can be overridden to implement a
            different form data parsing.
        json_module: A custom json decoding/encoding module, it should
            have `dump`, `dumps`, `load`, and `loads` methods
    """

    body_class = Body
    form_data_parser_class = FormDataParser
    json_module = json
    lock_class = asyncio.Lock

    def __init__(
        self,
        method: str,
        scheme: str,
        path: str,
        query_string: bytes,
        headers: Headers,
        root_path: str,
        http_version: str,
        scope: HTTPScope,
        *,
        max_content_length: Optional[int] = None,
        body_timeout: Optional[int] = None,
        send_push_promise: Callable[[str, Headers], Awaitable[None]],
    ) -> None:
        """Create a request object.

        Arguments:
            method: The HTTP verb.
            scheme: The scheme used for the request.
            path: The full unquoted path of the request.
            query_string: The raw bytes for the query string part.
            headers: The request headers.
            root_path: The root path that should be prepended to all
                routes.
            http_version: The HTTP version of the request.
            body: An awaitable future for the body data i.e.
                ``data = await body``
            max_content_length: The maximum length in bytes of the
                body (None implies no limit in Quart).
            body_timeout: The maximum time (seconds) to wait for the
                body before timing out.
            send_push_promise: An awaitable to send a push promise based
                off of this request (HTTP/2 feature).
            scope: Underlying ASGI scope dictionary.
        """
        super().__init__(
            method, scheme, path, query_string, headers, root_path, http_version, scope
        )
        self.body_timeout = body_timeout
        self.body = self.body_class(self.content_length, max_content_length)
        self._cached_json: Dict[bool, Any] = {False: Ellipsis, True: Ellipsis}
        self._form: Optional[MultiDict] = None
        self._files: Optional[MultiDict] = None
        self._parsing_lock = self.lock_class()
        self._send_push_promise = send_push_promise

    @property
    async def stream(self) -> NoReturn:
        raise NotImplementedError("Use body instead")

    @property
    async def data(self) -> bytes:
        return await self.get_data(as_text=False, parse_form_data=True)

    @overload
    async def get_data(self, cache: bool, as_text: Literal[False], parse_form_data: bool) -> bytes:
        ...

    @overload
    async def get_data(self, cache: bool, as_text: Literal[True], parse_form_data: bool) -> str:
        ...

    @overload
    async def get_data(
        self, cache: bool = True, as_text: bool = False, parse_form_data: bool = False
    ) -> AnyStr:
        ...

    async def get_data(
        self, cache: bool = True, as_text: bool = False, parse_form_data: bool = False
    ) -> AnyStr:
        """Get the request body data.

        Arguments:
            cache: If False the body data will be cleared, resulting in any
                subsequent calls returning an empty AnyStr and reducing
                memory usage.
            as_text: If True the data is returned as a decoded string,
                otherwise raw bytes are returned.
            parse_form_data: Parse the data as form data first, return any
                remaining data.
        """
        if parse_form_data:
            await self._load_form_data()

        try:
            raw_data = await asyncio.wait_for(self.body, timeout=self.body_timeout)
        except asyncio.TimeoutError:
            raise RequestTimeout()
        else:
            if not cache:
                self.body.clear()

            if as_text:
                return raw_data.decode(self.charset, self.encoding_errors)
            else:
                return raw_data

    @property
    async def values(self) -> CombinedMultiDict:
        sources = [self.args]
        if self.method != "GET":
            # Whilst GET requests are allowed to have a body, most
            # implementations do not allow this hence this
            # inconsistency may result in confusing values.
            form = await self.form
            sources.append(form)

        multidict_sources: List[MultiDict] = []
        for source in sources:
            if not isinstance(source, MultiDict):
                multidict_sources.append(MultiDict(source))
            else:
                multidict_sources.append(source)

        return CombinedMultiDict(multidict_sources)

    @property
    async def form(self) -> MultiDict:
        """The parsed form encoded data.

        Note file data is present in the :attr:`files`.
        """
        await self._load_form_data()
        return self._form

    @property
    async def files(self) -> MultiDict:
        """The parsed files.

        This will return an empty multidict unless the request
        mimetype was ``enctype="multipart/form-data"`` and the method
        POST, PUT, or PATCH.
        """
        await self._load_form_data()
        return self._files

    def make_form_data_parser(self) -> FormDataParser:
        return self.form_data_parser_class(
            charset=self.charset,
            errors=self.encoding_errors,
            max_content_length=self.max_content_length,
            cls=self.parameter_storage_class,
        )

    async def _load_form_data(self) -> None:
        async with self._parsing_lock:
            if self._form is None:
                parser = self.make_form_data_parser()
                try:
                    self._form, self._files = await asyncio.wait_for(
                        parser.parse(
                            self.body,
                            self.mimetype,
                            self.content_length,
                            self.mimetype_params,
                        ),
                        timeout=self.body_timeout,
                    )
                except asyncio.TimeoutError:
                    raise RequestTimeout()

    @property
    async def json(self) -> Any:
        return await self.get_json()

    async def get_json(self, force: bool = False, silent: bool = False, cache: bool = True) -> Any:
        """Parses the body data as JSON and returns it.

        Arguments:
            force: Force JSON parsing even if the mimetype is not JSON.
            silent: Do not trigger error handling if parsing fails, without
                this the :meth:`on_json_loading_failed` will be called on
                error.
            cache: Cache the parsed JSON on this request object.
        """
        if cache and self._cached_json[silent] is not Ellipsis:
            return self._cached_json[silent]

        if not (force or self.is_json):
            return None

        data = await self.get_data(cache=cache, as_text=True)

        try:
            result = self.json_module.loads(data)
        except ValueError as error:
            if silent:
                result = None
            else:
                result = self.on_json_loading_failed(error)

        if cache:
            self._cached_json[silent] = result

        return result

    def on_json_loading_failed(self, error: Exception) -> Any:
        """Handle a JSON parsing error.

        Arguments:
            error: The exception raised during parsing.
        Returns:
            Any value returned (if overridden) will be used as the
            default for any get_json calls.
        """
        if current_app and current_app.debug:
            raise BadRequest(f"Failed to decode JSON: {error}")

        raise BadRequest()

    async def send_push_promise(self, path: str) -> None:
        headers = Headers()
        for name in SERVER_PUSH_HEADERS_TO_COPY:
            for value in self.headers.getlist(name):
                headers.add(name, value)
        await self._send_push_promise(path, headers)
