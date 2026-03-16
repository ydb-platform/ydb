import io
import logging
import urllib.parse
from abc import ABC, abstractmethod
from datetime import timedelta
from types import TracebackType
from typing import (Any, BinaryIO, Callable, Dict, Iterable, Iterator, List,
                    Optional, Type, Union)

import requests
import requests.adapters

from . import useragent
from .casing import Casing
from .clock import Clock, RealClock
from .errors import DatabricksError, _ErrorCustomizer, _Parser
from .logger import RoundTrip
from .retries import retried

logger = logging.getLogger("databricks.sdk")


def _fix_host_if_needed(host: Optional[str]) -> Optional[str]:
    if not host:
        return host

    # Add a default scheme if it's missing
    if "://" not in host:
        host = "https://" + host

    o = urllib.parse.urlparse(host)
    # remove trailing slash
    path = o.path.rstrip("/")
    # remove port if 443
    netloc = o.netloc
    if o.port == 443:
        netloc = netloc.split(":")[0]

    return urllib.parse.urlunparse((o.scheme, netloc, path, o.params, o.query, o.fragment))


class _BaseClient:

    def __init__(
        self,
        debug_truncate_bytes: Optional[int] = None,
        retry_timeout_seconds: Optional[int] = None,
        user_agent_base: Optional[str] = None,
        header_factory: Optional[Callable[[], dict]] = None,
        max_connection_pools: Optional[int] = None,
        max_connections_per_pool: Optional[int] = None,
        pool_block: Optional[bool] = True,
        http_timeout_seconds: Optional[float] = None,
        extra_error_customizers: Optional[List[_ErrorCustomizer]] = None,
        debug_headers: Optional[bool] = False,
        clock: Optional[Clock] = None,
        streaming_buffer_size: int = 1024 * 1024,
    ):  # 1MB
        """
        :param debug_truncate_bytes:
        :param retry_timeout_seconds:
        :param user_agent_base:
        :param header_factory: A function that returns a dictionary of headers to include in the request.
        :param max_connection_pools: Number of urllib3 connection pools to cache before discarding the least
            recently used pool. Python requests default value is 10.
        :param max_connections_per_pool: The maximum number of connections to save in the pool. Improves performance
            in multithreaded situations. For now, we're setting it to the same value as connection_pool_size.
        :param pool_block: If pool_block is False, then more connections will are created, but not saved after the
            first use. Blocks when no free connections are available. urllib3 ensures that no more than
            pool_maxsize connections are used at a time. Prevents platform from flooding. By default, requests library
            doesn't block.
        :param http_timeout_seconds:
        :param extra_error_customizers:
        :param debug_headers: Whether to include debug headers in the request log.
        :param clock: Clock object to use for time-related operations.
        :param streaming_buffer_size: The size of the buffer to use for streaming responses.
        """

        self._debug_truncate_bytes = debug_truncate_bytes or 96
        self._debug_headers = debug_headers
        self._retry_timeout_seconds = retry_timeout_seconds or 300
        self._user_agent_base = user_agent_base or useragent.to_string()
        self._header_factory = header_factory
        self._clock = clock or RealClock()
        self._session = requests.Session()
        self._session.auth = self._authenticate
        self._streaming_buffer_size = streaming_buffer_size

        # We don't use `max_retries` from HTTPAdapter to align with a more production-ready
        # retry strategy established in the Databricks SDK for Go. See _is_retryable and
        # @retried for more details.
        http_adapter = requests.adapters.HTTPAdapter(
            pool_connections=max_connections_per_pool or 20,
            pool_maxsize=max_connection_pools or 20,
            pool_block=pool_block,
        )
        self._session.mount("https://", http_adapter)

        # Default to 60 seconds
        self._http_timeout_seconds = http_timeout_seconds or 60

        self._error_parser = _Parser(
            extra_error_customizers=extra_error_customizers,
            debug_headers=debug_headers,
        )

    def _authenticate(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        if self._header_factory:
            headers = self._header_factory()
            for k, v in headers.items():
                r.headers[k] = v
        return r

    @staticmethod
    def _fix_query_string(query: Optional[dict] = None) -> Optional[dict]:
        # Convert True -> "true" for Databricks APIs to understand booleans.
        # See: https://github.com/databricks/databricks-sdk-py/issues/142
        if query is None:
            return None
        with_fixed_bools = {k: v if type(v) != bool else ("true" if v else "false") for k, v in query.items()}

        # Query parameters may be nested, e.g.
        # {'filter_by': {'user_ids': [123, 456]}}
        # The HTTP-compatible representation of this is
        # filter_by.user_ids=123&filter_by.user_ids=456
        # To achieve this, we convert the above dictionary to
        # {'filter_by.user_ids': [123, 456]}
        # See the following for more information:
        # https://cloud.google.com/endpoints/docs/grpc-service-config/reference/rpc/google.api#google.api.HttpRule
        def flatten_dict(d: Dict[str, Any]) -> Dict[str, Any]:
            for k1, v1 in d.items():
                if isinstance(v1, dict):
                    v1 = dict(flatten_dict(v1))
                    for k2, v2 in v1.items():
                        yield f"{k1}.{k2}", v2
                else:
                    yield k1, v1

        flattened = dict(flatten_dict(with_fixed_bools))
        return flattened

    @staticmethod
    def _is_seekable_stream(data) -> bool:
        if data is None:
            return False
        if not isinstance(data, io.IOBase):
            return False
        return data.seekable()

    def do(
        self,
        method: str,
        url: str,
        query: Optional[dict] = None,
        headers: Optional[dict] = None,
        body: Optional[dict] = None,
        raw: bool = False,
        files=None,
        data=None,
        auth: Optional[Callable[[requests.PreparedRequest], requests.PreparedRequest]] = None,
        response_headers: Optional[List[str]] = None,
    ) -> Union[dict, list, BinaryIO]:
        if headers is None:
            headers = {}
        headers["User-Agent"] = self._user_agent_base

        # Wrap strings and bytes in a seekable stream so that we can rewind them.
        if isinstance(data, (str, bytes)):
            data = io.BytesIO(data.encode("utf-8") if isinstance(data, str) else data)

        if not data:
            # The request is not a stream.
            call = retried(
                timeout=timedelta(seconds=self._retry_timeout_seconds),
                is_retryable=self._is_retryable,
                clock=self._clock,
            )(self._perform)
        elif self._is_seekable_stream(data):
            # Keep track of the initial position of the stream so that we can rewind to it
            # if we need to retry the request.
            initial_data_position = data.tell()

            def rewind():
                logger.debug(f"Rewinding input data to offset {initial_data_position} before retry")
                data.seek(initial_data_position)

            call = retried(
                timeout=timedelta(seconds=self._retry_timeout_seconds),
                is_retryable=self._is_retryable,
                clock=self._clock,
                before_retry=rewind,
            )(self._perform)
        else:
            # Do not retry if the stream is not seekable. This is necessary to avoid bugs
            # where the retry doesn't re-read already read data from the stream.
            logger.debug(f"Retry disabled for non-seekable stream: type={type(data)}")
            call = self._perform

        response = call(
            method,
            url,
            query=query,
            headers=headers,
            body=body,
            raw=raw,
            files=files,
            data=data,
            auth=auth,
        )

        resp = dict()
        for header in response_headers if response_headers else []:
            resp[header] = response.headers.get(Casing.to_header_case(header))
        if raw:
            streaming_response = _StreamingResponse(response)
            streaming_response.set_chunk_size(self._streaming_buffer_size)
            resp["contents"] = streaming_response
            return resp
        if not len(response.content):
            return resp

        json_response = response.json()
        if json_response is None:
            return resp

        if isinstance(json_response, list):
            return json_response

        return {**resp, **json_response}

    @staticmethod
    def _is_retryable(err: BaseException) -> Optional[str]:
        # this method is Databricks-specific port of urllib3 retries
        # (see https://github.com/urllib3/urllib3/blob/main/src/urllib3/util/retry.py)
        # and Databricks SDK for Go retries
        # (see https://github.com/databricks/databricks-sdk-go/blob/main/apierr/errors.go)
        from urllib3.exceptions import ProxyError

        if isinstance(err, ProxyError):
            err = err.original_error
        if isinstance(err, requests.ConnectionError):
            # corresponds to `connection reset by peer` and `connection refused` errors from Go,
            # which are generally related to the temporary glitches in the networking stack,
            # also caused by endpoint protection software, like ZScaler, to drop connections while
            # not yet authenticated.
            #
            # return a simple string for debug log readability, as `raise TimeoutError(...) from err`
            # will bubble up the original exception in case we reach max retries.
            return f"cannot connect"
        if isinstance(err, requests.Timeout):
            # corresponds to `TLS handshake timeout` and `i/o timeout` in Go.
            #
            # return a simple string for debug log readability, as `raise TimeoutError(...) from err`
            # will bubble up the original exception in case we reach max retries.
            return f"timeout"
        if isinstance(err, DatabricksError):
            message = str(err)
            transient_error_string_matches = [
                "com.databricks.backend.manager.util.UnknownWorkerEnvironmentException",
                "does not have any associated worker environments",
                "There is no worker environment with id",
                "Unknown worker environment",
                "ClusterNotReadyException",
                "Unexpected error",
                "Please try again later or try a faster operation.",
                "RPC token bucket limit has been exceeded",
            ]
            for substring in transient_error_string_matches:
                if substring not in message:
                    continue
                return f"matched {substring}"
        return None

    def _perform(
        self,
        method: str,
        url: str,
        query: Optional[dict] = None,
        headers: Optional[dict] = None,
        body: Optional[dict] = None,
        raw: bool = False,
        files=None,
        data=None,
        auth: Callable[[requests.PreparedRequest], requests.PreparedRequest] = None,
    ):
        response = self._session.request(
            method,
            url,
            params=self._fix_query_string(query),
            json=body,
            headers=headers,
            files=files,
            data=data,
            auth=auth,
            stream=raw,
            timeout=self._http_timeout_seconds,
        )
        self._record_request_log(response, raw=raw or data is not None or files is not None)
        error = self._error_parser.get_api_error(response)
        if error is not None:
            raise error from None

        return response

    def _record_request_log(self, response: requests.Response, raw: bool = False) -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug(RoundTrip(response, self._debug_headers, self._debug_truncate_bytes, raw).generate())


class _RawResponse(ABC):

    @abstractmethod
    # follows Response signature: https://github.com/psf/requests/blob/main/src/requests/models.py#L799
    def iter_content(self, chunk_size: int = 1, decode_unicode: bool = False):
        pass

    @abstractmethod
    def close(self):
        pass


class _StreamingResponse(BinaryIO):
    _response: _RawResponse
    _buffer: bytes
    _content: Union[Iterator[bytes], None]
    _chunk_size: Union[int, None]
    _closed: bool = False

    def fileno(self) -> int:
        return 0

    def flush(self) -> int:  # type: ignore
        return 0

    def __init__(self, response: _RawResponse, chunk_size: Union[int, None] = None):
        self._response = response
        self._buffer = b""
        self._content = None
        self._chunk_size = chunk_size

    def _open(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if not self._content:
            self._content = self._response.iter_content(chunk_size=self._chunk_size, decode_unicode=False)

    def __enter__(self) -> BinaryIO:
        self._open()
        return self

    def set_chunk_size(self, chunk_size: Union[int, None]) -> None:
        self._chunk_size = chunk_size

    def close(self) -> None:
        self._response.close()
        self._closed = True

    def isatty(self) -> bool:
        return False

    def read(self, n: int = -1) -> bytes:
        """
        Read up to n bytes from the response stream. If n is negative, read
        until the end of the stream.
        """

        self._open()
        read_everything = n < 0
        remaining_bytes = n
        res = b""
        while remaining_bytes > 0 or read_everything:
            if len(self._buffer) == 0:
                try:
                    self._buffer = next(self._content)
                except StopIteration:
                    break
            bytes_available = len(self._buffer)
            to_read = bytes_available if read_everything else min(remaining_bytes, bytes_available)
            res += self._buffer[:to_read]
            self._buffer = self._buffer[to_read:]
            remaining_bytes -= to_read
        return res

    def readable(self) -> bool:
        return self._content is not None

    def readline(self, __limit: int = ...) -> bytes:
        raise NotImplementedError()

    def readlines(self, __hint: int = ...) -> List[bytes]:
        raise NotImplementedError()

    def seek(self, __offset: int, __whence: int = ...) -> int:
        raise NotImplementedError()

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        raise NotImplementedError()

    def truncate(self, __size: Union[int, None] = ...) -> int:
        raise NotImplementedError()

    def writable(self) -> bool:
        return False

    def write(self, s: Union[bytes, bytearray]) -> int:  # type: ignore
        raise NotImplementedError()

    def writelines(self, lines: Iterable[bytes]) -> None:  # type: ignore
        raise NotImplementedError()

    def __next__(self) -> bytes:
        return self.read(1)

    def __iter__(self) -> Iterator[bytes]:
        return self._content

    def __exit__(
        self,
        t: Union[Type[BaseException], None],
        value: Union[BaseException, None],
        traceback: Union[TracebackType, None],
    ) -> None:
        self._content = None
        self._buffer = b""
        self.close()
