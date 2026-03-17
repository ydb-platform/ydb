from typing import Union

import httpx

from pytest_httpx._httpx_internals import _proxy_url
from pytest_httpx._request_matcher import _RequestMatcher


class RequestDescription:
    def __init__(
        self,
        real_transport: Union[httpx.BaseTransport, httpx.AsyncBaseTransport],
        request: httpx.Request,
        matchers: list[_RequestMatcher],
    ):
        self.real_transport = real_transport
        self.request = request

        headers_encoding = request.headers.encoding
        self.expected_headers = {
            # httpx uses lower cased header names as internal key
            header.lower().encode(headers_encoding)
            for matcher in matchers
            if matcher.headers
            for header in matcher.headers
        }
        self.expect_body = any([matcher.expect_body() for matcher in matchers])
        self.expect_proxy = any([matcher.proxy_url is not None for matcher in matchers])
        self.expected_extensions = {
            extension
            for matcher in matchers
            if matcher.extensions
            for extension in matcher.extensions
        }

    def __str__(self) -> str:
        request_description = f"{self.request.method} request on {self.request.url}"
        if extra_description := self.extra_request_description():
            request_description += f" with {extra_description}"
        return request_description

    def extra_request_description(self) -> str:
        extra_description = []

        if self.expected_headers:
            headers_encoding = self.request.headers.encoding
            present_headers = {}
            # Can be cleaned based on the outcome of https://github.com/encode/httpx/discussions/2841
            for name, lower_name, value in self.request.headers._list:
                if lower_name in self.expected_headers:
                    name = name.decode(headers_encoding)
                    if name in present_headers:
                        present_headers[name] += f", {value.decode(headers_encoding)}"
                    else:
                        present_headers[name] = value.decode(headers_encoding)

            extra_description.append(f"{present_headers} headers")

        if self.expect_body:
            extra_description.append(f"{self.request.read()} body")

        if self.expect_proxy:
            proxy_url = _proxy_url(self.real_transport)
            extra_description.append(f"{proxy_url if proxy_url else 'no'} proxy URL")

        if self.expected_extensions:
            present_extensions = {
                name: value
                for name, value in self.request.extensions.items()
                if name in self.expected_extensions
            }
            extra_description.append(f"{present_extensions} extensions")

        return " and ".join(extra_description)
