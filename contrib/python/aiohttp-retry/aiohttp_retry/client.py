from __future__ import annotations

import asyncio
import logging
import sys
from abc import abstractmethod
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Generator,
    List,
    Tuple,
    Union,
)

from aiohttp import ClientResponse, ClientSession, hdrs
from aiohttp.typedefs import StrOrURL
from yarl import URL as YARL_URL

from .retry_options import ExponentialRetry, RetryOptionsBase

_MIN_SERVER_ERROR_STATUS = 500

if TYPE_CHECKING:
    from types import TracebackType

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol


class _Logger(Protocol):
    """_Logger defines which methods logger object should have."""

    @abstractmethod
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        pass


# url itself or list of urls for changing between retries
_RAW_URL_TYPE = Union[StrOrURL, YARL_URL]
_URL_TYPE = Union[_RAW_URL_TYPE, List[_RAW_URL_TYPE], Tuple[_RAW_URL_TYPE, ...]]
_LoggerType = Union[_Logger, logging.Logger]

RequestFunc = Callable[..., Awaitable[ClientResponse]]


@dataclass
class RequestParams:
    method: str
    url: _RAW_URL_TYPE
    headers: dict[str, Any] | None = None
    trace_request_ctx: dict[str, Any] | None = None
    kwargs: dict[str, Any] | None = None


class _RequestContext:
    def __init__(
        self,
        request_func: RequestFunc,
        params_list: list[RequestParams],
        logger: _LoggerType,
        retry_options: RetryOptionsBase,
        raise_for_status: bool = False,
    ) -> None:
        assert len(params_list) > 0  # noqa: S101

        self._request_func = request_func
        self._params_list = params_list
        self._logger = logger
        self._retry_options = retry_options
        self._raise_for_status = raise_for_status

        self._response: ClientResponse | None = None

    async def _is_skip_retry(self, current_attempt: int, response: ClientResponse) -> bool:
        if current_attempt == self._retry_options.attempts:
            return True

        if response.method.upper() not in self._retry_options.methods:
            return True

        if response.status >= _MIN_SERVER_ERROR_STATUS and self._retry_options.retry_all_server_errors:
            return False

        if response.status in self._retry_options.statuses:
            return False

        if self._retry_options.evaluate_response_callback is None:
            return True

        return await self._retry_options.evaluate_response_callback(response)

    async def _do_request(self) -> ClientResponse:
        current_attempt = 0

        while True:
            self._logger.debug(f"Attempt {current_attempt+1} out of {self._retry_options.attempts}")

            current_attempt += 1
            try:
                try:
                    params = self._params_list[current_attempt - 1]
                except IndexError:
                    params = self._params_list[-1]

                response: ClientResponse = await self._request_func(
                    params.method,
                    params.url,
                    headers=params.headers,
                    trace_request_ctx={
                        "current_attempt": current_attempt,
                        **(params.trace_request_ctx or {}),
                    },
                    **(params.kwargs or {}),
                )

                debug_message = f"Retrying after response code: {response.status}"
                skip_retry = await self._is_skip_retry(current_attempt, response)

                if skip_retry:
                    if self._raise_for_status:
                        response.raise_for_status()
                    self._response = response
                    return self._response
                retry_wait = self._retry_options.get_timeout(attempt=current_attempt, response=response)

            except Exception as e:
                if current_attempt >= self._retry_options.attempts:
                    raise

                is_exc_valid = any(isinstance(e, exc) for exc in self._retry_options.exceptions)
                if not is_exc_valid:
                    raise

                debug_message = f"Retrying after exception: {e!r}"
                retry_wait = self._retry_options.get_timeout(attempt=current_attempt, response=None)

            self._logger.debug(debug_message)
            await asyncio.sleep(retry_wait)

    def __await__(self) -> Generator[Any, None, ClientResponse]:
        return self.__aenter__().__await__()

    async def __aenter__(self) -> ClientResponse:
        return await self._do_request()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._response is not None and not self._response.closed:
            self._response.close()


def _url_to_urls(url: _URL_TYPE) -> tuple[StrOrURL, ...]:
    if isinstance(url, (str, YARL_URL)):
        return (url,)

    if isinstance(url, list):
        urls = tuple(url)
    elif isinstance(url, tuple):
        urls = url
    else:
        msg = "you can pass url only by str or list/tuple"  # type: ignore[unreachable]
        raise ValueError(msg)  # noqa: TRY004

    if len(urls) == 0:
        msg = "you can pass url by str or list/tuple with attempts count size"
        raise ValueError(msg)

    return urls


class RetryClient:
    def __init__(
        self,
        client_session: ClientSession | None = None,
        logger: _LoggerType | None = None,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if client_session is not None:
            client = client_session
            closed = None
        else:
            client = ClientSession(*args, **kwargs)
            closed = False

        self._client = client
        self._closed = closed

        self._logger: _LoggerType = logger or logging.getLogger("aiohttp_retry")
        self._retry_options: RetryOptionsBase = retry_options or ExponentialRetry()
        self._raise_for_status = raise_for_status

    @property
    def retry_options(self) -> RetryOptionsBase:
        return self._retry_options

    def requests(
        self,
        params_list: list[RequestParams],
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
    ) -> _RequestContext:
        return self._make_requests(
            params_list=params_list,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
        )

    def request(
        self,
        method: str,
        url: StrOrURL,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        return self._make_request(
            method=method,
            url=url,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
            **kwargs,
        )

    def get(
        self,
        url: _URL_TYPE,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        return self._make_request(
            method=hdrs.METH_GET,
            url=url,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
            **kwargs,
        )

    def options(
        self,
        url: _URL_TYPE,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        return self._make_request(
            method=hdrs.METH_OPTIONS,
            url=url,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
            **kwargs,
        )

    def head(
        self,
        url: _URL_TYPE,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        return self._make_request(
            method=hdrs.METH_HEAD,
            url=url,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
            **kwargs,
        )

    def post(
        self,
        url: _URL_TYPE,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        return self._make_request(
            method=hdrs.METH_POST,
            url=url,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
            **kwargs,
        )

    def put(
        self,
        url: _URL_TYPE,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        return self._make_request(
            method=hdrs.METH_PUT,
            url=url,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
            **kwargs,
        )

    def patch(
        self,
        url: _URL_TYPE,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        return self._make_request(
            method=hdrs.METH_PATCH,
            url=url,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
            **kwargs,
        )

    def delete(
        self,
        url: _URL_TYPE,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        return self._make_request(
            method=hdrs.METH_DELETE,
            url=url,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
            **kwargs,
        )

    async def close(self) -> None:
        await self._client.close()
        self._closed = True

    def _make_request(
        self,
        method: str,
        url: _URL_TYPE,
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
        **kwargs: Any,
    ) -> _RequestContext:
        url_list = _url_to_urls(url)
        params_list = [
            RequestParams(
                method=method,
                url=url,
                headers=kwargs.pop("headers", {}),
                trace_request_ctx=kwargs.pop("trace_request_ctx", None),
                kwargs=kwargs,
            )
            for url in url_list
        ]

        return self._make_requests(
            params_list=params_list,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
        )

    def _make_requests(
        self,
        params_list: list[RequestParams],
        retry_options: RetryOptionsBase | None = None,
        raise_for_status: bool | None = None,
    ) -> _RequestContext:
        if retry_options is None:
            retry_options = self._retry_options
        if raise_for_status is None:
            raise_for_status = self._raise_for_status
        return _RequestContext(
            request_func=self._client.request,
            params_list=params_list,
            logger=self._logger,
            retry_options=retry_options,
            raise_for_status=raise_for_status,
        )

    async def __aenter__(self) -> RetryClient:  # noqa: PYI034
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    def __del__(self) -> None:
        if getattr(self, "_closed", None) is None:
            # in case object was not initialized (__init__ raised an exception)
            return

        if not self._closed:
            self._logger.warning("Aiohttp retry client was not closed")
