import json
import re
from typing import Optional, Union, Any
from re import Pattern

import httpx
from httpx import QueryParams

from pytest_httpx._httpx_internals import _proxy_url
from pytest_httpx._options import _HTTPXMockOptions


def _url_match(
    url_to_match: Union[Pattern[str], httpx.URL],
    received: httpx.URL,
    params: Optional[dict[str, Union[str | list[str]]]],
) -> bool:
    if isinstance(url_to_match, re.Pattern):
        return url_to_match.match(str(received)) is not None

    # Compare query parameters apart as order of parameters should not matter
    received_params = to_params_dict(received.params)
    if params is None:
        params = to_params_dict(url_to_match.params)

    # Remove the query parameters from the original URL to compare everything besides query parameters
    received_url = received.copy_with(query=None)
    url = url_to_match.copy_with(query=None)

    return (received_params == params) and (url == received_url)


def to_params_dict(params: QueryParams) -> dict[str, Union[str | list[str]]]:
    """Convert query parameters to a dict where the value is a string if the parameter has a single value and a list of string otherwise."""
    d = {}
    for key in params:
        values = params.get_list(key)
        d[key] = values if len(values) > 1 else values[0]
    return d


class _RequestMatcher:
    def __init__(
        self,
        options: _HTTPXMockOptions,
        url: Optional[Union[str, Pattern[str], httpx.URL]] = None,
        method: Optional[str] = None,
        proxy_url: Optional[Union[str, Pattern[str], httpx.URL]] = None,
        match_headers: Optional[dict[str, Any]] = None,
        match_content: Optional[bytes] = None,
        match_json: Optional[Any] = None,
        match_data: Optional[dict[str, Any]] = None,
        match_files: Optional[Any] = None,
        match_extensions: Optional[dict[str, Any]] = None,
        match_params: Optional[dict[str, Union[str | list[str]]]] = None,
        is_optional: Optional[bool] = None,
        is_reusable: Optional[bool] = None,
    ):
        self._options = options
        self.nb_calls = 0
        self.url = httpx.URL(url) if url and isinstance(url, str) else url
        self.method = method.upper() if method else method
        self.headers = match_headers
        self.content = match_content
        self.json = match_json
        self.data = match_data
        self.files = match_files
        self.params = match_params
        self.proxy_url = (
            httpx.URL(proxy_url)
            if proxy_url and isinstance(proxy_url, str)
            else proxy_url
        )
        self.extensions = match_extensions
        self.is_optional = (
            not options.assert_all_responses_were_requested
            if is_optional is None
            else is_optional
        )
        self.is_reusable = (
            options.can_send_already_matched_responses
            if is_reusable is None
            else is_reusable
        )
        if self._is_matching_body_more_than_one_way():
            raise ValueError(
                "Only one way of matching against the body can be provided. "
                "If you want to match against the JSON decoded representation, use match_json. "
                "If you want to match against the multipart representation, use match_files (and match_data). "
                "Otherwise, use match_content."
            )
        if self.params and not self.url:
            raise ValueError("URL must be provided when match_params is used.")
        if self.params and isinstance(self.url, re.Pattern):
            raise ValueError(
                "match_params cannot be used in addition to regex URL. Request this feature via https://github.com/Colin-b/pytest_httpx/issues/new?title=Regex%20URL%20should%20allow%20match_params&body=Hi,%20I%20need%20a%20regex%20to%20match%20the%20non%20query%20part%20of%20the%20URL%20only"
            )
        if self._is_matching_params_more_than_one_way():
            raise ValueError(
                "Provided URL must not contain any query parameter when match_params is used."
            )
        if self.data and not self.files:
            raise ValueError(
                "match_data is meant to be used for multipart matching (in conjunction with match_files)."
                "Use match_content to match url encoded data."
            )

    def expect_body(self) -> bool:
        matching_ways = [
            self.content is not None,
            self.json is not None,
            self.files is not None,
        ]
        return sum(matching_ways) == 1

    def _is_matching_body_more_than_one_way(self) -> bool:
        matching_ways = [
            self.content is not None,
            self.json is not None,
            self.files is not None,
        ]
        return sum(matching_ways) > 1

    def _is_matching_params_more_than_one_way(self) -> bool:
        url_has_params = (
            bool(self.url.params)
            if (self.url and isinstance(self.url, httpx.URL))
            else False
        )
        matching_ways = [
            self.params is not None,
            url_has_params,
        ]
        return sum(matching_ways) > 1

    def match(
        self,
        real_transport: Union[httpx.HTTPTransport, httpx.AsyncHTTPTransport],
        request: httpx.Request,
    ) -> bool:
        return (
            self._url_match(request)
            and self._method_match(request)
            and self._headers_match(request)
            and self._content_match(request)
            and self._proxy_match(real_transport)
            and self._extensions_match(request)
        )

    def _url_match(self, request: httpx.Request) -> bool:
        if not self.url:
            return True

        return _url_match(self.url, request.url, self.params)

    def _method_match(self, request: httpx.Request) -> bool:
        if not self.method:
            return True

        return request.method == self.method

    def _headers_match(self, request: httpx.Request) -> bool:
        if not self.headers:
            return True

        encoding = request.headers.encoding
        request_headers = {}
        # Can be cleaned based on the outcome of https://github.com/encode/httpx/discussions/2841
        for raw_name, raw_value in request.headers.raw:
            if raw_name in request_headers:
                request_headers[raw_name] += b", " + raw_value
            else:
                request_headers[raw_name] = raw_value

        return all(
            request_headers.get(header_name.encode(encoding))
            == header_value.encode(encoding)
            for header_name, header_value in self.headers.items()
        )

    def _content_match(self, request: httpx.Request) -> bool:
        if self.content is not None:
            return request.content == self.content

        if self.json is not None:
            try:
                # httpx._content.encode_json hard codes utf-8 encoding.
                return json.loads(request.content.decode("utf-8")) == self.json
            except json.decoder.JSONDecodeError:
                return False

        if self.files:
            if not (
                boundary_matched := re.match(b"^--([0-9a-f]*)\r\n", request.content)
            ):
                return False
            # Ensure we re-use the same boundary for comparison
            boundary = boundary_matched.group(1)
            # Prevent internal httpx changes from impacting users not matching on files
            from httpx._multipart import MultipartStream

            multipart_content = b"".join(
                MultipartStream(self.data or {}, self.files, boundary)
            )
            return request.content == multipart_content

        return True

    def _proxy_match(
        self, real_transport: Union[httpx.HTTPTransport, httpx.AsyncHTTPTransport]
    ) -> bool:
        if not self.proxy_url:
            return True

        if real_proxy_url := _proxy_url(real_transport):
            return _url_match(self.proxy_url, real_proxy_url, params=None)

        return False

    def _extensions_match(self, request: httpx.Request) -> bool:
        if not self.extensions:
            return True

        return all(
            request.extensions.get(extension_name) == extension_value
            for extension_name, extension_value in self.extensions.items()
        )

    def should_have_matched(self) -> bool:
        """Return True if the matcher did not serve its purpose."""
        return not self.is_optional and not self.nb_calls

    def __str__(self) -> str:
        if self.is_reusable:
            matcher_description = f"Match {self.method or 'every'} request"
        else:
            matcher_description = "Already matched" if self.nb_calls else "Match"
            matcher_description += f" {self.method or 'any'} request"
        if self.url:
            matcher_description += f" on {self.url}"
        if extra_description := self._extra_description():
            matcher_description += f" with {extra_description}"
        return matcher_description

    def _extra_description(self) -> str:
        extra_description = []

        if self.params:
            extra_description.append(f"{self.params} query parameters")
        if self.headers:
            extra_description.append(f"{self.headers} headers")
        if self.content is not None:
            extra_description.append(f"{self.content} body")
        if self.json is not None:
            extra_description.append(f"{self.json} json body")
        if self.data is not None:
            extra_description.append(f"{self.data} multipart data")
        if self.files is not None:
            extra_description.append(f"{self.files} files")
        if self.proxy_url:
            extra_description.append(f"{self.proxy_url} proxy URL")
        if self.extensions:
            extra_description.append(f"{self.extensions} extensions")

        return " and ".join(extra_description)
