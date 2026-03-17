from __future__ import annotations

import typing
import email.parser
from http import client

from urllib3.util.url import parse_url
from urllib3.connection import HTTPConnection, HTTPSConnection
try:
    from urllib3.connection import _TYPE_BODY
except ImportError:
    _TYPE_BODY = typing.Union[bytes, typing.IO[typing.Any], typing.Iterable[bytes], str]

from .__version__ import __title__


class RawHTTPResponse(client.HTTPResponse):
    # Added Feature https://github.com/realgam3/requests-raw/issues/5
    def begin(self):
        # Fixes Bug https://github.com/realgam3/requests-raw/issues/1
        self._method = self._method or __title__.upper()

        if self.headers is not None:
            # we've already started reading the response
            return

        line = self.fp.peek()
        if not line.startswith(b"HTTP/"):
            self.code = self.status = 0
            self.reason = "Non Standard"
            self.version = 0
            self.headers = self.msg = email.parser.Parser(_class=client.HTTPMessage).parsestr("")
            self.length = None
            self.chunked = False
            self.will_close = True
            return

        return super().begin()


class RawHTTPConnection(HTTPConnection):
    response_class = RawHTTPResponse

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__method = None

    def request(
            self,
            method: str,
            url: str,
            body: _TYPE_BODY | None = None,
            headers: typing.Mapping[str, str] | None = None,
            *,
            chunked: bool = False,
            preload_content: bool = True,
            decode_content: bool = True,
            enforce_content_length: bool = True,
    ) -> None:
        self.__method = method.lower()
        # HTTP Proxy
        if self.__method == __title__ and not url.startswith("/"):
            _url = parse_url(url)
            _body = body.split(b"/", 1)
            body = b"".join([_body[0], f"{_url.scheme}://{_url.netloc}/".encode(), _body[1]])

        return super().request(
            method, url, body, headers,
            chunked=chunked, preload_content=preload_content,
            decode_content=decode_content, enforce_content_length=enforce_content_length
        )

    def putrequest(
            self,
            method: str,
            url: str,
            skip_host: bool = False,
            skip_accept_encoding: bool = False,
    ) -> None:
        if self.__method != __title__:
            return super().putrequest(
                method, url,
                skip_host=skip_host, skip_accept_encoding=skip_accept_encoding
            )

        if self._HTTPConnection__response and self._HTTPConnection__response.isclosed():
            self._HTTPConnection__response = None

        if self._HTTPConnection__state == client._CS_IDLE:
            self._HTTPConnection__state = client._CS_REQ_STARTED
        else:
            raise client.CannotSendRequest(self._HTTPConnection__state)

    def putheader(self, header: str, *values: str) -> None:
        if self.__method != __title__:
            return super().putheader(header, *values)

        if self._HTTPConnection__state != client._CS_REQ_STARTED:
            raise client.CannotSendHeader()

    def endheaders(self, message_body=None, **kwargs):
        if self.__method != __title__:
            return super().endheaders(message_body=message_body, **kwargs)

        if self._HTTPConnection__state == client._CS_REQ_STARTED:
            self._HTTPConnection__state = client._CS_REQ_SENT
        else:
            raise client.CannotSendHeader()


class RawHTTPSConnection(RawHTTPConnection, HTTPSConnection):
    pass
