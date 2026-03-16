import hashlib
import io
import json
import os
import random
import socket
import ssl
import threading
import time
import urllib.parse
from types import TracebackType
from typing import (
    Any,
    Callable,
    Iterator,
    Mapping,
    NamedTuple,
    Protocol,
    Sequence,
    Union,
    runtime_checkable,
)

import filelock
import urllib3
from urllib3.util.retry import Retry

from blobfile import _xml as xml

CHUNK_SIZE = 8 * 2**20

DEFAULT_CONNECTION_POOL_MAX_SIZE = 32
DEFAULT_MAX_CONNECTION_POOL_COUNT = 10
DEFAULT_REDIRECT_RETRY_COUNT = int(os.getenv("BLOBFILE_REDIRECT_RETRY_COUNT", "0"))

PARALLEL_COPY_MINIMUM_PART_SIZE = 32 * 2**20

EARLY_EXPIRATION_SECONDS = 5 * 60

INVALID_HOSTNAME_STATUS = 600  # fake status for invalid hostname

BACKOFF_INITIAL = 0.1
BACKOFF_MAX = 60.0

HOSTNAME_EXISTS = 0
HOSTNAME_DOES_NOT_EXIST = 1
HOSTNAME_STATUS_UNKNOWN = 2

GCP_BASE_URL = "https://storage.googleapis.com"

DEFAULT_RETRY_CODES = (408, 429, 500, 502, 503, 504)


# https://github.com/christopher-hesse/blobfile/issues/153
# https://github.com/christopher-hesse/blobfile/issues/156
COMMON_ERROR_SUBSTRINGS = ["[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC]", "('Connection aborted.',"]

rng = random.SystemRandom()


def exponential_sleep_generator(
    initial: float = BACKOFF_INITIAL, maximum: float = BACKOFF_MAX, multiplier: float = 2
) -> Iterator[float]:
    # retry once immediately in case it's a transient error
    yield 0
    base = initial
    while True:
        # https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
        yield base * rng.random()
        base *= multiplier
        if base > maximum:
            base = maximum


class Request:
    """
    A struct representing an HTTP request
    """

    def __init__(
        self,
        method: str,
        url: str,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        data: Any = None,
        preload_content: bool = True,
        success_codes: Sequence[int] = (200,),
        # https://cloud.google.com/storage/docs/resumable-uploads#practices
        retry_codes: Sequence[int] = DEFAULT_RETRY_CODES,
    ) -> None:
        self.method: str = method
        self.url: str = url
        self.params: Mapping[str, str] | None = params
        self.headers: Mapping[str, str] | None = headers
        self.data: Any = data
        self.preload_content: bool = preload_content
        self.success_codes: Sequence[int] = success_codes
        self.retry_codes: Sequence[int] = retry_codes

    def __repr__(self) -> str:
        return f"<Request method={self.method} url={self.url} params={self.params}>"


class FileBody:
    """
    A struct for referencing a section of a file on disk to be used as the `data` property
    on a Request
    """

    def __init__(self, path: str, start: int, end: int) -> None:
        self.path: str = path
        self.start: int = start
        self.end: int = end

    def __repr__(self):
        return f"<FileBody path={self.path} start={self.start} end={self.end}>"


def build_url(base_url: str, template: str, **data: str) -> str:
    escaped_data = {}
    for k, v in data.items():
        escaped_data[k] = urllib.parse.quote(v, safe="")
    return base_url + template.format(**escaped_data)


class Error(Exception):
    """Base class for blobfile exceptions."""

    def __init__(self, message: str, *args: Any):
        self.message: str = message
        super().__init__(message, *args)


def _extract_error(data: bytes) -> tuple[str | None, str | None]:
    if data.startswith(b"\xef\xbb\xbf<?xml"):
        try:
            result = xml.parse(data)
            return result["Error"]["Code"], result["Error"].get("Message")
        except Exception:
            pass
    elif data.startswith(b"{"):
        try:
            result = json.loads(data)
            return str(result["error"]), result.get("error_description")
        except Exception:
            pass
    return None, None


def _extract_error_from_response(
    response: "urllib3.BaseHTTPResponse",
) -> tuple[str | None, str | None, str | None]:
    err = None
    err_desc = None
    err_headers = None
    # TODO: type checker thinks response.data is never None, confirm this
    if response.data is not None:  # type: ignore
        err, err_desc = _extract_error(response.data)
    if response.headers:
        err_headers = ", ".join(f"{header}: {value}" for header, value in response.headers.items())
    return err, err_desc, err_headers


class RequestFailure(Error):
    """
    A request failed, possibly after some number of retries
    """

    def __init__(
        self,
        message: str,
        request_string: str,
        response_status: int,
        error: str | None,
        error_description: str | None,
        error_headers: str | None = None,
    ):
        self.request_string: str = request_string
        self.response_status: int = response_status
        self.error: str | None = error
        self.error_description: str | None = error_description
        self.error_headers: str | None = error_headers
        super().__init__(
            message,
            self.request_string,
            self.response_status,
            self.error,
            self.error_description,
            self.error_headers,
        )

    def __str__(self) -> str:
        return (
            f"message={self.message}, request={self.request_string}, "
            f"status={self.response_status}, error={self.error}, "
            f"error_description={self.error_description}, error_headers={self.error_headers}"
        )

    @classmethod
    def create_from_request_response(
        cls, message: str, request: Request, response: "urllib3.BaseHTTPResponse"
    ) -> Any:
        # this helper function exists because if you make a custom Exception subclass it cannot
        # be unpickled easily: https://stackoverflow.com/questions/41808912/cannot-unpickle-exception-subclass
        err, err_desc, err_headers = _extract_error_from_response(response)
        # use string representation since request may not be serializable
        # exceptions need to be serializable when raised from subprocesses
        return cls(
            message=message,
            request_string=str(request),
            response_status=response.status,
            error=err,
            error_description=err_desc,
            error_headers=err_headers,
        )


class RestartableStreamingWriteFailure(RequestFailure):
    """
    A streaming write failed in a permanent way that requires restarting from the beginning of the stream
    """

    pass


class ConcurrentWriteFailure(RequestFailure):
    """
    A write failed due to another concurrent writer
    """

    pass


class DeadlineExceeded(RequestFailure):
    """
    A read failed after the deadline was exceeded
    """

    pass


class VersionMismatch(RequestFailure):
    """
    A write failed due to a version mismatch, using ETag for azure or generation for gcloud
    """

    pass


class Stat(NamedTuple):
    size: int
    mtime: float
    ctime: float
    md5: str | None
    version: str | None


class DirEntry(NamedTuple):
    path: str
    name: str
    is_dir: bool
    is_file: bool
    stat: Stat | None


class PoolDirector:
    def __init__(self, connection_pool_max_size: int, max_connection_pool_count: int) -> None:
        self.connection_pool_max_size = connection_pool_max_size
        self.max_connection_pool_count = max_connection_pool_count
        self.pool_manager = None
        self.creation_pid = None
        self.lock = threading.Lock()

    def get_http_pool(self) -> urllib3.PoolManager:
        # ssl is not fork safe https://docs.python.org/2/library/ssl.html#multi-processing
        # urllib3 may not be fork safe https://github.com/urllib3/urllib3/issues/1179
        # both are supposedly threadsafe though, so we shouldn't need a thread-local pool
        with self.lock:
            if self.pool_manager is None or self.creation_pid != os.getpid():
                self.creation_pid = os.getpid()
                self.pool_manager = urllib3.PoolManager(
                    maxsize=self.connection_pool_max_size, num_pools=self.max_connection_pool_count
                )
                # for debugging with mitmproxy
                # self.http = urllib3.ProxyManager('http://localhost:8080/', ssl_context=context)
            return self.pool_manager

    # we don't want to serialize locks or other unpicklable objects
    # when this object is passed to concurrent.futures executors
    def __getstate__(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if k not in ["lock", "pool_manager"]}

    def __setstate__(self, state: Any) -> None:
        self.__init__(
            connection_pool_max_size=state["connection_pool_max_size"],
            max_connection_pool_count=state["max_connection_pool_count"],
        )
        self.__dict__.update(state)


# we used to have a per-config instance of this class, but that is a bit annoying when using ProcessPoolExecutor
# as the pool will be reset when the config is passed to the executor
# so instead, default to this global director
global_pool_director = PoolDirector(
    connection_pool_max_size=DEFAULT_CONNECTION_POOL_MAX_SIZE,
    max_connection_pool_count=DEFAULT_MAX_CONNECTION_POOL_COUNT,
)


class Config:
    def __init__(
        self,
        log_callback: Callable[[str], None],
        connection_pool_max_size: int,
        max_connection_pool_count: int,
        azure_write_chunk_size: int,
        google_write_chunk_size: int,
        retry_log_threshold: int,
        retry_common_log_threshold: int,
        retry_limit: int | None,
        connect_timeout: int | None,
        read_timeout: int | None,
        output_az_paths: bool,
        use_azure_storage_account_key_fallback: bool,
        get_http_pool: Callable[[], urllib3.PoolManager] | None,
        use_streaming_read: bool,
        use_blind_writes: bool,
        default_buffer_size: int,
        get_deadline: Callable[[], float | None] | None,
        save_access_token_to_disk: bool,
        multiprocessing_start_method: str,
    ) -> None:
        self.log_callback = log_callback
        self.connection_pool_max_size = connection_pool_max_size
        self.max_connection_pool_count = max_connection_pool_count
        self.azure_write_chunk_size = azure_write_chunk_size
        self.retry_log_threshold = retry_log_threshold
        self.retry_common_log_threshold = retry_common_log_threshold
        self.retry_limit = retry_limit
        self.google_write_chunk_size = google_write_chunk_size
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.output_az_paths = output_az_paths
        self.use_azure_storage_account_key_fallback = use_azure_storage_account_key_fallback
        self.use_streaming_read = use_streaming_read
        self.use_blind_writes = use_blind_writes
        self.default_buffer_size = default_buffer_size
        self.get_deadline = get_deadline
        self.save_access_token_to_disk = save_access_token_to_disk
        self.multiprocessing_start_method = multiprocessing_start_method

        if get_http_pool is None:
            if (
                max_connection_pool_count != DEFAULT_MAX_CONNECTION_POOL_COUNT
                or connection_pool_max_size != DEFAULT_CONNECTION_POOL_MAX_SIZE
            ):
                log_callback(
                    "warning: max_connection_pool_count and connection_pool_max_size are no longer supported, set get_http_pool instead if you want to control http pooling"
                )
        self._get_http_pool = get_http_pool

    def get_http_pool(self) -> urllib3.PoolManager:
        if self._get_http_pool is None:
            return global_pool_director.get_http_pool()
        else:
            return self._get_http_pool()


class WindowedFile:
    """
    A file object that reads from a window into a file
    """

    def __init__(self, f: Any, start: int, end: int) -> None:
        self._f = f
        self._start = start
        self._end = end
        self._pos = -1
        self.seek(0)

    def tell(self) -> int:
        return self._pos - self._start

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> None:
        new_pos = self._start + offset
        assert whence == io.SEEK_SET and self._start <= new_pos < self._end
        self._f.seek(new_pos, whence)
        self._pos = new_pos

    def read(self, n: int | None = None) -> Any:
        assert self._pos <= self._end
        if n is None:
            n = self._end - self._pos
        n = min(n, self._end - self._pos)
        buf = self._f.read(n)
        self._pos += len(buf)
        if n > 0 and len(buf) == 0:
            raise Error("failed to read expected amount of data from file")
        assert self._pos <= self._end
        return buf


_hostname_check_cache: dict[str, tuple[float, int]] = {}


def _check_hostname(hostname: str) -> int:
    def inner(hostname: str) -> int:
        try:
            socket.getaddrinfo(hostname, None, family=socket.AF_INET)
        except socket.gaierror as e:
            if e.errno != socket.EAI_NONAME and e.errno != socket.EAI_NODATA:
                # we got some sort of other socket error, so it's unclear if the host exists or not
                return HOSTNAME_STATUS_UNKNOWN

            # On linux, most temporary name resolution failures return EAI_AGAIN, however it
            # appears that some EAI_NONAME can be triggered by SERVFAIL, instead of just NXDOMAIN
            # On other platforms, it's unclear how to differentiate between a temporary failure
            # and a permanent one.
            # Either way, assume that if we can lookup bing.com, then it's likely the hostname
            # does not exist
            try:
                socket.getaddrinfo("www.bing.com", None, family=socket.AF_INET)
            except socket.gaierror:
                # if we can't resolve bing, then the network is likely down and we don't know if
                # the hostname exists or not
                return HOSTNAME_STATUS_UNKNOWN

            # in this case, we could resolve bing, but not the original hostname. likely the
            # hostname does not exist (though this is definitely not a foolproof check)
            return HOSTNAME_DOES_NOT_EXIST

        # no errors encountered, the hostname exists
        return HOSTNAME_EXISTS

    # maybe this cache is a little bit overkill...
    now = time.time()
    if hostname in _hostname_check_cache and _hostname_check_cache[hostname][0] >= now:
        return _hostname_check_cache[hostname][1]
    ret = inner(hostname)
    _hostname_check_cache[hostname] = (now + 10, ret)
    return ret


class Timeout(Exception):
    pass


def _read_with_deadline(
    fp: Any, sock: socket.socket, nbytes_to_read: int, deadline: float
) -> bytes:
    timeout = deadline - time.time()
    if timeout <= 0:
        raise Timeout("Did not read enough bytes before deadline expired")
    sock.settimeout(timeout)
    try:
        # the peek is done with the hopes that this will get any buffered data
        initial_data = fp.peek()
    except TimeoutError:
        raise Timeout("Timed out waiting for read")

    buf = bytearray(nbytes_to_read)
    mv = memoryview(buf)
    mv[: len(initial_data)] = initial_data
    nbytes_read = len(initial_data)
    while nbytes_read < nbytes_to_read:
        timeout = deadline - time.time()
        if timeout <= 0:
            raise Timeout("Did not read enough bytes before deadline expired")
        sock.settimeout(timeout)
        try:
            n = sock.recv_into(mv[nbytes_read:])
        except TimeoutError:
            raise Timeout("Timed out waiting for read")
        nbytes_read += n
    return bytes(buf)


def execute_request(conf: Config, build_req: Callable[[], Request]) -> "urllib3.BaseHTTPResponse":
    for attempt, backoff in enumerate(exponential_sleep_generator()):
        req = build_req()
        url = req.url
        if req.params is not None:
            if len(req.params) > 0:
                url += "?" + urllib.parse.urlencode(req.params)

        f = None
        if isinstance(req.data, FileBody):
            f = open(req.data.path, "rb")
            body = WindowedFile(f, start=req.data.start, end=req.data.end)
        else:
            body = req.data

        preload_content = req.preload_content
        deadline = None
        total_timeout = None

        if conf.get_deadline is not None and req.method != "HEAD":
            # HEAD requests don't have a body which is the only part the deadline applies to anyway
            deadline = conf.get_deadline()
            if deadline is not None:
                assert preload_content, "preload_content must be set to True when using a deadline"
                preload_content = False
                now = time.time()
                if now >= deadline:
                    raise DeadlineExceeded.create_from_request_response(
                        message="refusing to send request due to deadline",
                        request=req,
                        response=urllib3.response.HTTPResponse(status=0, body=io.BytesIO(b"")),
                    )
                # this isn't actually sufficient, since the read timeout doesn't work in the way we want
                # it's still possible for the header to be returned really slowly, but since we mostly
                # talk to only a few servers, it's possible they all send the header quickly
                total_timeout = deadline - now

        redirect = False
        retries: Retry | bool = False
        if DEFAULT_REDIRECT_RETRY_COUNT > 0:
            redirect = True
            retries = Retry(
                total=DEFAULT_REDIRECT_RETRY_COUNT,
                connect=0,
                read=0,
                status=0,
                redirect=DEFAULT_REDIRECT_RETRY_COUNT,
                raise_on_status=False,
                raise_on_redirect=False,
                backoff_factor=0.0,
            )

        err = None
        try:
            resp = conf.get_http_pool().request(
                method=req.method,
                url=url,
                headers=req.headers,
                # WindowedFile isn't actually an IO[Any] or Iterable[bytes]
                body=body,  # type: ignore
                timeout=urllib3.Timeout(
                    connect=conf.connect_timeout, read=conf.read_timeout, total=total_timeout
                ),
                preload_content=preload_content,
                retries=retries,
                redirect=redirect,
            )
            if deadline is not None:
                if resp.headers.get("Transfer-Encoding") == "chunked":
                    # we don't support this, just read the body
                    resp.data
                else:
                    # read the data manually since it's hard to implement a deadline using urllib3
                    # this is all super hacky
                    # https://github.com/urllib3/urllib3/issues/1741
                    # because this is an SSLSocket, we can't easily recreate it from the fileno(), instead find the underlying socket object
                    fp: Any = resp._fp.fp  # type: ignore
                    sock: socket.socket = fp.raw._sock
                    try:
                        body = _read_with_deadline(
                            fp=fp,
                            sock=sock,
                            nbytes_to_read=int(resp.headers["Content-Length"]),
                            deadline=deadline,
                        )
                        # since we successfully read the data, release the connection back to the pool
                        resp.release_conn()
                        # create a new resp object with the body we read
                        resp = urllib3.response.HTTPResponse(
                            status=resp.status, headers=resp.headers, body=body
                        )
                    except Timeout as e:
                        resp.close()
                        raise DeadlineExceeded.create_from_request_response(
                            message=f"request failed with exception {e}", request=req, response=resp
                        )
            if resp.status in req.success_codes:
                return resp
            else:
                message = f"unexpected status {resp.status}"
                error_class = RequestFailure
                if url.startswith(GCP_BASE_URL):
                    if resp.status in (429, 503):
                        message += ": if you are writing a blob this error may be due to multiple concurrent writers - make sure you are not writing to the same blob from multiple processes simultaneously"
                    elif resp.status == 401:
                        message += ": no valid credentials were found, please login with 'gcloud auth application-default login' or else set the 'GOOGLE_APPLICATION_CREDENTIALS' environment variable to the path of a JSON format service account key"
                    elif resp.status == 403:
                        message += ": credentials were found but do not grant access to this resource, please make sure the account you are using (either via 'gcloud auth application-default login' or the 'GOOGLE_APPLICATION_CREDENTIALS' environment variable) has access"
                elif (
                    resp.status == 412 and resp.headers.get("x-ms-error-code") == "ConditionNotMet"
                ):
                    error_class = VersionMismatch
                    message = "etag mismatch"
                err = error_class.create_from_request_response(
                    message=message, request=req, response=resp
                )
                if resp.status not in req.retry_codes:
                    raise err
        except (
            urllib3.exceptions.ConnectTimeoutError,
            urllib3.exceptions.ReadTimeoutError,
            urllib3.exceptions.ProtocolError,
            # we should probably only catch SSLErrors matching `DECRYPTION_FAILED_OR_BAD_RECORD_MAC`
            # but it's not obvious what the error code will be from the logs
            # and because we are connecting to known servers, it's likely that non-transient
            # SSL errors will be rare, so for now catch all SSLErrors
            urllib3.exceptions.SSLError,
            # urllib3 wraps all errors in its own exception classes
            # but seems to miss ssl.SSLError
            # https://github.com/urllib3/urllib3/blob/9971e27e83a891ba7b832fa9e5d2f04bbcb1e65f/src/urllib3/response.py#L415
            # https://github.com/urllib3/urllib3/blame/9971e27e83a891ba7b832fa9e5d2f04bbcb1e65f/src/urllib3/response.py#L437
            # https://github.com/urllib3/urllib3/issues/1764
            ssl.SSLError,
        ) as e:
            if isinstance(e, urllib3.exceptions.NewConnectionError):
                # azure accounts have unique urls and it's hard to tell apart
                # an invalid hostname from a network error
                url = urllib.parse.urlparse(req.url)
                assert url.hostname is not None
                if (
                    url.hostname.endswith(".blob.core.windows.net")
                    and _check_hostname(url.hostname) == HOSTNAME_DOES_NOT_EXIST
                ):
                    # in order to handle the azure failures in some sort-of-reasonable way
                    # create a fake response that has a special status code we can
                    # handle just like a 404
                    fake_resp = urllib3.response.HTTPResponse(
                        status=INVALID_HOSTNAME_STATUS,
                        body=io.BytesIO(b""),  # avoid error when using "with resp:"
                    )
                    if fake_resp.status in req.success_codes:
                        return fake_resp
                    else:
                        raise RequestFailure.create_from_request_response(
                            "host does not exist", request=req, response=fake_resp
                        ) from e

            err = RequestFailure.create_from_request_response(
                message=f"request failed with exception {e}",
                request=req,
                response=urllib3.response.HTTPResponse(status=0, body=io.BytesIO(b"")),
            )
        finally:
            if f is not None:
                f.close()

        if conf.retry_limit is not None and attempt >= conf.retry_limit:
            raise err

        if attempt >= get_log_threshold_for_error(conf, str(err)):
            conf.log_callback(
                f"error {err} when executing http request {req} attempt {attempt}, sleeping for {backoff:.1f} seconds before retrying"
            )
        time.sleep(backoff)
    assert False, "unreachable"


class TupleEncoder(json.JSONEncoder):
    def encode(self, o: Any) -> Any:
        def hint_tuples(item: Any) -> Any:
            if isinstance(item, tuple):
                return {"__tuple__": True, "items": item}
            if isinstance(item, list):
                return [hint_tuples(e) for e in item]
            if isinstance(item, dict):
                return {key: hint_tuples(value) for key, value in item.items()}
            else:
                return item

        return super().encode(hint_tuples(o))


def hinted_tuple_hook(obj: Any) -> Any:
    if "__tuple__" in obj:
        return tuple(obj["items"])
    else:
        return obj


class TokenManager:
    """
    Automatically refresh tokens when they expire
    """

    def __init__(self, get_token_fn: Callable[[Config, Any], tuple[Any, float]], name: str) -> None:
        self._get_token_fn = get_token_fn
        self._tokens = {}
        self._expirations = {}
        self._lock = threading.Lock()
        self._access_lock_file = os.path.expanduser(f"~/.blobfile/{name}_tokens.lock")
        self._access_token_file = os.path.expanduser(f"~/.blobfile/{name}_tokens.json")

    def _load_token_file(self):
        if os.path.exists(self._access_token_file):
            with open(self._access_token_file) as f:
                tokens = json.load(f, object_hook=hinted_tuple_hook)
                for key, value in zip(tokens["token_keys"], tokens["token_values"]):
                    self._tokens[key] = value
                for key, value in zip(tokens["expiration_keys"], tokens["expiration_values"]):
                    self._expirations[key] = value

    def _save_token_file(self, log_callback: Callable[[str], None]):
        os.makedirs(os.path.dirname(self._access_lock_file), exist_ok=True)

        try:
            with filelock.FileLock(self._access_lock_file, timeout=1):
                os.makedirs(os.path.dirname(self._access_token_file), exist_ok=True)
                # Use a ".tmp" in same dir to prevent cross device issues with os.replace
                tmp_path = self._access_token_file + ".tmp"
                with open(tmp_path, "w") as f:
                    f.write(
                        TupleEncoder().encode(
                            {
                                "token_keys": list(self._tokens.keys()),
                                "token_values": list(self._tokens.values()),
                                "expiration_keys": list(self._expirations.keys()),
                                "expiration_values": list(self._expirations.values()),
                            }
                        )
                    )
                os.replace(tmp_path, self._access_token_file)
        except filelock.Timeout:
            log_callback("Another instance of this application currently holds the lock.")

    def get_token(self, conf: Config, key: Any) -> Any:
        with self._lock:
            now = time.time()
            expiration = self._expirations.get(key)
            if expiration is None or (now + EARLY_EXPIRATION_SECONDS) > expiration:
                if conf.save_access_token_to_disk:
                    # Grab the access token file
                    self._load_token_file()
                    # See if an update already occurred
                    expiration = self._expirations.get(key)
                    if expiration is None or (now + EARLY_EXPIRATION_SECONDS) > expiration:
                        # If not, get a new token and update the access file
                        self._tokens[key], self._expirations[key] = self._get_token_fn(conf, key)

                        self._save_token_file(conf.log_callback)

                else:
                    self._tokens[key], self._expirations[key] = self._get_token_fn(conf, key)
                assert self._expirations[key] is not None

            assert key in self._tokens
            return self._tokens[key]


class BaseStreamingWriteFile(io.BufferedIOBase):
    def __init__(self, conf: Config, chunk_size: int, partial_writes_on_exc: bool) -> None:
        self._offset = 0
        # contents waiting to be uploaded
        self._buf = bytearray()
        self._chunk_size = chunk_size
        self._conf = conf

        self.partial_writes_on_exc = partial_writes_on_exc
        self.had_exception: bool = False

    def _upload_chunk(self, chunk: memoryview, finalize: bool) -> None:
        raise NotImplementedError

    def _upload_buf(self, buf: memoryview, finalize: bool = False) -> int:
        if finalize:
            size = len(buf)
        else:
            size = (len(buf) // self._chunk_size) * self._chunk_size
            assert size > 0

        try:
            chunk = buf[:size]
            self._upload_chunk(chunk, finalize)
            self._offset += len(chunk)
        finally:
            del chunk, buf  # pyright: ignore[reportPossiblyUnboundVariable]
        return size

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # Store the exception so that in close we can decide whether or not to write the file.
        self.had_exception = exc_val is not None
        super().__exit__(exc_type, exc_val, exc_tb)

    def close(self) -> None:
        if self.closed:
            return

        if not self.had_exception or self.partial_writes_on_exc:
            # we will have a partial remaining buffer at this point, upload it
            size = self._upload_buf(memoryview(self._buf), finalize=True)
            assert size == len(self._buf)

        # Always clear the buffer whether or not we uploaded the file
        self._buf = bytearray()
        super().close()

    def tell(self) -> int:
        return self._offset + len(self._buf)

    def writable(self) -> bool:
        return True

    def write(self, b: bytes) -> int:  # type: ignore
        if len(self._buf) == 0 and len(b) >= self._chunk_size:
            # optimization for when we want to do a single large f.write()
            mv = memoryview(b)
            size = self._upload_buf(mv)
            # only append the part we were not able to upload
            self._buf = bytearray(mv[size:])
        else:
            self._buf += b
            if len(self._buf) >= self._chunk_size:
                try:
                    mv = memoryview(self._buf)
                    size = self._upload_buf(mv)
                    self._buf = bytearray(mv[size:])
                finally:
                    del mv  # pyright: ignore[reportPossiblyUnboundVariable]
        assert len(self._buf) < self._chunk_size
        return len(b)

    def readinto(self, b: Any) -> int:
        raise io.UnsupportedOperation("not readable")

    def detach(self) -> io.RawIOBase:
        raise io.UnsupportedOperation("no underlying raw stream")

    def read1(self, size: int = -1) -> bytes:
        raise io.UnsupportedOperation("not readable")

    def readinto1(self, b: Any) -> int:
        raise io.UnsupportedOperation("not readable")


class BaseStreamingReadFile(io.RawIOBase):
    def __init__(self, conf: Config, path: str, size: int) -> None:
        super().__init__()
        self._conf = conf
        self._size = size
        self._path = path
        # current reading byte offset in the file
        self._offset = 0
        self._f = None
        self.requests = 0
        self.failures = 0
        self.bytes_read = 0

    def _request_chunk(
        self, streaming: bool, start: int, end: int | None = None
    ) -> "urllib3.BaseHTTPResponse":
        raise NotImplementedError

    def readall(self) -> bytes:
        # https://github.com/christopher-hesse/blobfile/issues/46
        # due to a limitation of the ssl module, we cannot read more than 2**31 bytes at a time
        # reading a huge file in a single request is probably a bad idea anyway since the request
        # cannot be retried without re-reading the entire requested amount
        # instead, read into a buffer and return the buffer
        pieces = []
        while True:
            bytes_remaining = self._size - self._offset
            assert bytes_remaining >= 0, "read more bytes than expected"
            # if a user doesn't like this value, it is easy to use .read(size) directly
            opt_piece = self.read(min(CHUNK_SIZE, bytes_remaining))
            assert opt_piece is not None, "file is in non-blocking mode"
            piece = opt_piece
            if len(piece) == 0:
                break
            pieces.append(piece)
        return b"".join(pieces)

    # https://bugs.python.org/issue27501
    def readinto(self, b: Any) -> int | None:
        bytes_remaining = self._size - self._offset
        if bytes_remaining <= 0 or len(b) == 0:
            return 0

        # make sure we can slice the memoryview below
        if not isinstance(b, memoryview):
            b = memoryview(b)

        if len(b) > bytes_remaining:
            # if we get a file that was larger than we expected, don't read the extra data
            b = b[:bytes_remaining]

        n = 0  # for pyright
        if self._conf.use_streaming_read:
            for attempt, backoff in enumerate(exponential_sleep_generator()):
                if self._f is None:
                    resp = self._request_chunk(streaming=True, start=self._offset)
                    if resp.status == 416:
                        # likely the file was truncated while we were reading it
                        # return an empty string
                        return 0
                    self._f = resp
                    self.requests += 1

                err = None
                try:
                    # urllib3 should actually admit a memoryview over here
                    opt_n = self._f.readinto(b)  # type: ignore
                    assert opt_n is not None, "file is in non-blocking mode"
                    n = opt_n
                    if n == 0:
                        # assume that the connection has died
                        # if the file was truncated, we'll try to open it again and end up
                        # returning out of this loop
                        err = Error(
                            f"failed to read from connection while reading file at {self._path}"
                        )
                    else:
                        # only break out if we successfully read at least one byte
                        break
                except (
                    urllib3.exceptions.ReadTimeoutError,  # haven't seen this error here, but seems possible
                    urllib3.exceptions.ProtocolError,
                    urllib3.exceptions.SSLError,
                    ssl.SSLError,
                ) as e:
                    err = Error(f"exception {e} while reading file at {self._path}")
                # assume that the connection has died or is in an unusable state
                # we don't want to put a broken connection back in the pool
                # so don't call self._f.release_conn()
                self._f.close()
                self._f = None
                self.failures += 1

                if self._conf.retry_limit is not None and attempt >= self._conf.retry_limit:
                    raise err

                if attempt >= get_log_threshold_for_error(self._conf, str(err)):
                    self._conf.log_callback(
                        f"error {err} when executing readinto({len(b)}) at offset {self._offset} attempt {attempt}, sleeping for {backoff:.1f} seconds before retrying"
                    )
                time.sleep(backoff)
        else:
            resp = self._request_chunk(
                streaming=False, start=self._offset, end=self._offset + len(b)
            )
            if resp.status == 416:
                # likely the file was truncated while we were reading it
                # return an empty string
                return 0
            self.requests += 1
            n = len(resp.data)
            b[:n] = resp.data
        self.bytes_read += n
        self._offset += n
        return n

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_offset = offset
        elif whence == io.SEEK_CUR:
            new_offset = self._offset + offset
        elif whence == io.SEEK_END:
            new_offset = self._size + offset
        else:
            raise ValueError(
                f"Invalid whence ({whence}, should be {io.SEEK_SET}, {io.SEEK_CUR}, or {io.SEEK_END})"
            )
        if new_offset != self._offset:
            self._offset = new_offset
            if self._f is not None:
                self._f.close()
            self._f = None
        return self._offset

    def tell(self) -> int:
        return self._offset

    def close(self) -> None:
        if self.closed:
            return

        if hasattr(self, "_f") and self._f is not None:
            # normally we would return the connection to the pool at this point, but in rare
            # circumstances this can cause an invalid socket to be in the connection pool and
            # crash urllib3
            # https://github.com/urllib3/urllib3/issues/1878
            self._f.close()
            self._f = None

        super().close()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True


# this should by BinaryIO, but that produces an error error: Argument of type 'IO[Any]' cannot be assigned to parameter 'f' of type 'BinaryIO' when used with open()
def block_md5(f: Any) -> bytes:
    m = hashlib.md5()
    while True:
        block = f.read(CHUNK_SIZE)
        if block == b"":
            break
        m.update(block)
    return m.digest()


def calc_range(start: int | None = None, end: int | None = None) -> str:
    # https://cloud.google.com/storage/docs/xml-api/get-object-download
    # oddly range requests are not mentioned in the JSON API, only in the XML api
    if start is not None and end is not None:
        return f"bytes={start}-{end-1}"
    elif start is not None:
        return f"bytes={start}-"
    elif end is not None:
        if end > 0:
            return f"bytes=0-{end-1}"
        else:
            return f"bytes=-{-int(end)}"
    else:
        raise Error("Invalid range")


def strip_slashes(path: str) -> str:
    while path.endswith("/"):
        path = path[:-1]
    return path


def path_join(a: str, b: str) -> str:
    # urljoin has issues with : at any point and ; at the end of a url
    # instead do the same sort of logic as https://github.com/python/cpython/blob/e6fe10d34096a23be7d26271cf6aba429313b01d/Lib/urllib/parse.py#L514
    # but without the url stuff
    assert "://" not in a and "://" not in b
    if a == "":
        return b
    if b == "":
        return a

    if b[:1] == "/":
        segments = b.split("/")
    else:
        base_parts = a.split("/")
        if base_parts[-1] != "":
            del base_parts[-1]
        segments = base_parts + b.split("/")
        segments[1:-1] = [s for s in segments[1:-1] if s != ""]

    resolved_path: list[str] = []

    for seg in segments:
        if seg == "..":
            if len(resolved_path) > 0:
                resolved_path.pop()
        elif seg == ".":
            continue
        else:
            resolved_path.append(seg)

    if segments[-1] in (".", ".."):
        resolved_path.append("")

    return "/".join(resolved_path)


def get_log_threshold_for_error(conf: Config, err: str) -> int:
    if any(substr in err for substr in COMMON_ERROR_SUBSTRINGS):
        return conf.retry_common_log_threshold
    else:
        return conf.retry_log_threshold


@runtime_checkable
class BlobPathLike(Protocol):
    """Similar to the __fspath__ protocol, but for remote blob paths."""

    def __blobpath__(self) -> str: ...


RemoteOrLocalPath = Union[str, BlobPathLike, "os.PathLike[str]"]


def path_to_str(path: RemoteOrLocalPath) -> str:
    if isinstance(path, BlobPathLike):
        return path.__blobpath__()
    elif isinstance(path, os.PathLike):
        return path.__fspath__()
    else:
        return path
