import email
import json
import typing
import urllib.parse

import aiohttp.web

CONTENT_IN_GET_REQUEST_ERROR = (
    'GET requests cannot have content, but Content-Length header was sent.'
)
CHUNKED_CONTENT_IN_GET_REQUEST_ERROR = (
    "GET requests cannot have content, but 'Transfer-Encoding: chunked' "
    'header was sent.'
)
MULTIPART_MIME_PATTERN = """MIME-Version: 1.0
Content-Type: %s

%s"""


class BaseError(Exception):
    pass


class MockedError(BaseError):
    """Base class for mockserver mocked errors."""

    error_code = 'unknown'


class TimeoutError(MockedError):  # pylint: disable=redefined-builtin
    """Exception used to mock HTTP client timeout errors.

    Requires service side support.

    Available as ``mockserver.TimeoutError`` alias
    or by full name ``testsuite.utils.http.TimeoutError``.
    """

    error_code = 'timeout'


class NetworkError(MockedError):
    """Exception used to mock HTTP client network errors.

    Requires service side support.

    Available as ``mockserver.NetworkError`` alias
    or by full name ``testsuite.utils.http.NetworkError``.
    """

    error_code = 'network'


class HttpResponseError(BaseError):
    def __init__(self, *, url: str, status: int):
        self.url = url
        self.status = status
        super().__init__(f"status={self.status}, url='{self.url}'")


class InvalidRequestError(BaseError):
    """Invalid request which cannot be wrapped"""


class Request:
    """Adapts aiohttp.web.BaseRequest to mimic a frequently used subset of
    werkzeug.Request interface. ``data`` property is not supported,
    use get_data() instead.
    """

    def __init__(self, request: aiohttp.web.BaseRequest, data: bytes):
        self._request = request
        self._data: bytes = data
        self._json: object = None
        self._form: dict[str, str] | None = None

    @property
    def method(self) -> str:
        return self._request.method

    @property
    def url(self) -> str:
        return str(self._request.url)

    @property
    def path(self) -> str:
        return self._request.path

    # For backward compatibility with code using aiohttp.web.BaseRequest
    @property
    def path_qs(self) -> str:
        return self._request.raw_path

    @property
    def query_string(self) -> bytes:
        path_and_query = self._request.raw_path.split('?')
        if len(path_and_query) < 2:
            return b''
        return path_and_query[1].encode()

    @property
    def headers(self):
        return self._request.headers

    @property
    def content_type(self):
        return self._request.content_type

    def get_data(self) -> bytes:
        return self._data

    @property
    def form(self):
        if self._form is None:
            if self._request.content_type in (
                '',
                'application/x-www-form-urlencoded',
            ):
                charset = self._request.charset or 'utf-8'
                items = urllib.parse.parse_qsl(
                    self._data.rstrip().decode(charset),
                    keep_blank_values=True,
                    encoding=charset,
                )
                self._form = dict(items)
            elif self._request.content_type.startswith('multipart/form-data'):
                charset = self._request.charset or 'utf-8'
                epost_data = MULTIPART_MIME_PATTERN % (
                    self._request.headers['content-type'],
                    self._data.rstrip().decode(charset),
                )
                data = email.message_from_string(epost_data)
                assert data.is_multipart()

                self._form = {}
                for part in data.get_payload():
                    name = part.get_param('name', header='content-disposition')
                    payload = part.get_payload(decode=True).decode(charset)
                    try:
                        payload = int(payload)
                    except ValueError:
                        pass
                    self._form[name] = payload  # type: ignore[index]

            else:
                self._form = {}

        return self._form

    @property
    def json(self) -> typing.Any:
        if self._json is None:
            bytes_body = self.get_data()
            encoding = self._request.charset or 'utf-8'
            str_body = bytes_body.decode(encoding)
            self._json = json.loads(str_body)
        return self._json

    @property
    def cookies(self) -> typing.Mapping[str, str]:
        return self._request.cookies

    @property
    def args(self):
        return self._request.query

    # For backward compatibility with code using aiohttp.web.BaseRequest
    @property
    def query(self):
        return self._request.query


class _NoValue:
    pass


async def wrap_request(request: aiohttp.web.BaseRequest) -> Request:
    if request.method == 'GET':
        if request.content_length:
            raise InvalidRequestError(CONTENT_IN_GET_REQUEST_ERROR)
        if request.headers.get('Transfer-Encoding', '') == 'chunked':
            raise InvalidRequestError(CHUNKED_CONTENT_IN_GET_REQUEST_ERROR)
    if request.headers.get('expect') == '100-continue':
        await request.writer.write(b'HTTP/1.1 100 Continue\r\n\r\n')
        await request.writer.drain()
    data = await request.content.read()
    return Request(request, data)


class Response:
    def __init__(
        self,
        body: bytes | bytearray | None = None,
        text: str | None = None,
        status: int = 200,
        headers: typing.Mapping[str, str] | None = None,
        content_type: str | None = None,
        charset: str | None = None,
    ):
        if body and text:
            raise RuntimeError(
                'Response params "body" and "text" can not be used at the same time'
            )

        self._body = body
        self._text = text
        self._status = status
        self._headers = headers
        self._content_type = content_type
        self._charset = charset

    def __repr__(self):
        return (
            f'<{self.__class__.__name__} body={self._body!r} '
            f'text={self._text} status={self._status} content_type={self._content_type} charset={self._charset}>'
        )

    def to_aiohttp(self) -> aiohttp.web.Response:
        return aiohttp.web.Response(
            body=self._body,
            text=self._text,
            status=self._status,
            headers=self._headers,
            content_type=self._content_type,
            charset=self._charset,
        )


class ClientResponse:
    def __init__(
        self,
        response: aiohttp.ClientResponse,
        content: bytes,
        *,
        json_loads,
    ):
        self._response = response
        self._content: bytes = content
        self._text: str | None = None
        self._form: dict[str, str] | None = None
        self._json_loads = json_loads

    def __repr__(self):
        return (
            f'<{self.__class__.__name__} method={self._response.method} '
            f'url={self._response.url} status={self.status} content={self.content!r}>'
        )

    @property
    def status_code(self) -> int:
        return self._response.status

    # For backward compatibility with code using async ClientResponse
    @property
    def status(self) -> int:
        return self._response.status

    @property
    def reason(self) -> str | None:
        return self._response.reason

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def text(self) -> str:
        if self._text is None:
            encoding = self._response.get_encoding()
            self._text = str(self._content, encoding)
        return self._text

    def json(self) -> typing.Any:
        return self._json_loads(self.text)

    @property
    def form(self):
        if self._form is None:
            if self.content_type in ('', 'application/x-www-form-urlencoded'):
                items = urllib.parse.parse_qsl(
                    self.text,
                    keep_blank_values=True,
                    encoding=self.encoding,
                )
                self._form = dict(items)
            else:
                self._form = {}

        return self._form

    @property
    def headers(self):
        return self._response.headers

    @property
    def content_type(self):
        return self._response.content_type

    @property
    def encoding(self):
        return self._response.get_encoding()

    @property
    def cookies(self):
        return self._response.cookies

    def raise_for_status(self) -> None:
        if self._response.status < 400:
            return
        self._response.release()
        raise HttpResponseError(
            url=str(self._response.request_info.url),
            status=self._response.status,
        )


async def wrap_client_response(
    response: aiohttp.ClientResponse,
    *,
    json_loads=json.loads,
):
    content = await response.read()
    wrapped = ClientResponse(response, content, json_loads=json_loads)
    return wrapped


def make_response(
    response: str | bytes | bytearray | None = None,
    status: int = 200,
    headers: typing.Mapping[str, str] | None = None,
    content_type: str | None = None,
    charset: str | None = None,
    *,
    json=_NoValue,
    form=_NoValue,
) -> Response:
    """
    Create HTTP response object. Returns ``Response`` instance.

    :param response: response content
    :param status: HTTP status code
    :param headers: HTTP headers dictionary
    :param content_type: HTTP Content-Type header
    :param charset: Response character set
    :param json: JSON response shortcut
    :param form: x-www-form-urlencoded response shortcut
    """
    if json is not _NoValue and form is not _NoValue:
        raise RuntimeError(
            'Response params "json" and "form" can not be used '
            'at the same time',
        )
    if json is not _NoValue:
        response = _json_response(json)
        if content_type is None:
            content_type = 'application/json'
    if form is not _NoValue:
        response = _form_response(form)
        if content_type is None:
            content_type = 'application/x-www-form-urlencoded'

    if isinstance(response, (bytes, bytearray)):
        return Response(
            body=response,
            status=status,
            headers=headers,
            content_type=content_type,
            charset=charset,
        )
    if isinstance(response, str):
        return Response(
            text=response,
            status=status,
            headers=headers,
            content_type=content_type,
            charset=charset,
        )
    if response is None:
        return Response(
            headers=headers,
            status=status,
            content_type=content_type,
            charset=charset,
        )
    raise RuntimeError(f'Unsupported response {response!r} given')


def _json_response(data: typing.Any) -> bytes:
    text = json.dumps(data, ensure_ascii=False)
    return text.encode('utf-8')


def _form_response(data: typing.Any) -> bytes:
    text = urllib.parse.urlencode(data)
    return text.encode('utf-8')
