import copy
import inspect
from typing import Union, Optional, Callable, Any, NoReturn
from collections.abc import Awaitable

import httpx

from pytest_httpx import _httpx_internals
from pytest_httpx._options import _HTTPXMockOptions
from pytest_httpx._pretty_print import RequestDescription
from pytest_httpx._request_matcher import _RequestMatcher


class HTTPXMock:
    """
    This class is only exposed for `httpx_mock` fixture type hinting purpose.
    """

    def __init__(self, options: _HTTPXMockOptions) -> None:
        """Private and subject to breaking changes without notice."""
        self._options = options
        self._requests: list[
            tuple[Union[httpx.HTTPTransport, httpx.AsyncHTTPTransport], httpx.Request]
        ] = []
        self._callbacks: list[
            tuple[
                _RequestMatcher,
                Callable[
                    [httpx.Request],
                    Union[
                        Optional[httpx.Response], Awaitable[Optional[httpx.Response]]
                    ],
                ],
            ]
        ] = []
        self._requests_not_matched: list[httpx.Request] = []

    def add_response(
        self,
        status_code: int = 200,
        http_version: str = "HTTP/1.1",
        headers: Optional[_httpx_internals.HeaderTypes] = None,
        content: Optional[bytes] = None,
        text: Optional[str] = None,
        html: Optional[str] = None,
        stream: Any = None,
        json: Any = None,
        **matchers: Any,
    ) -> None:
        """
        Mock the response that will be sent if a request match.

        :param status_code: HTTP status code of the response to send. Default to 200 (OK).
        :param http_version: HTTP protocol version of the response to send. Default to HTTP/1.1
        :param headers: HTTP headers of the response to send. Default to no headers.
        :param content: HTTP body of the response (as bytes).
        :param text: HTTP body of the response (as string).
        :param html: HTTP body of the response (as HTML string content).
        :param stream: HTTP body of the response (as httpx.SyncByteStream or httpx.AsyncByteStream) as stream content.
        :param json: HTTP body of the response (if JSON should be used as content type) if data is not provided.
        :param url: Full URL identifying the request(s) to match. Use in addition to match_params if you do not want to provide query parameters as part of the URL.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param method: HTTP method identifying the request(s) to match.
        :param proxy_url: Full proxy URL identifying the request(s) to match.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param match_headers: HTTP headers identifying the request(s) to match. Must be a dictionary.
        :param match_content: Full HTTP body identifying the request(s) to match. Must be bytes.
        :param match_json: JSON decoded HTTP body identifying the request(s) to match. Must be JSON encodable.
        :param match_data: Multipart data (excluding files) identifying the request(s) to match. Must be a dictionary.
        :param match_files: Multipart files identifying the request(s) to match. Refer to httpx documentation for more information on supported values: https://www.python-httpx.org/advanced/clients/#multipart-file-encoding
        :param match_extensions: Extensions identifying the request(s) to match. Must be a dictionary.
        :param match_params: Query string parameters identifying the request(s) to match (if not provided as part of URL already). Must be a dictionary with str keys (parameter name) and str values (or a list of str values if parameter is provided more than once).
        :param is_optional: True will mark this response as optional, False will expect a request matching it. Must be a boolean. Default to the opposite of assert_all_responses_were_requested option value (itself defaulting to True, meaning this parameter default to False).
        :param is_reusable: True will allow re-using this response even if it already matched, False prevent re-using it. Must be a boolean. Default to the can_send_already_matched_responses option value (itself defaulting to False).
        """

        json = copy.deepcopy(json) if json is not None else None

        def response_callback(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status_code=status_code,
                extensions={"http_version": http_version.encode("ascii")},
                headers=headers,
                json=json,
                content=content,
                text=text,
                html=html,
                stream=stream,
            )

        self.add_callback(response_callback, **matchers)

    def add_callback(
        self,
        callback: Callable[
            [httpx.Request],
            Union[Optional[httpx.Response], Awaitable[Optional[httpx.Response]]],
        ],
        **matchers: Any,
    ) -> None:
        """
        Mock the action that will take place if a request match.

        :param callback: The callable that will be called upon reception of the matched request.
        It must expect one parameter, the received httpx.Request and should return a httpx.Response.
        :param url: Full URL identifying the request(s) to match. Use in addition to match_params if you do not want to provide query parameters as part of the URL.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param method: HTTP method identifying the request(s) to match.
        :param proxy_url: Full proxy URL identifying the request(s) to match.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param match_headers: HTTP headers identifying the request(s) to match. Must be a dictionary.
        :param match_content: Full HTTP body identifying the request(s) to match. Must be bytes.
        :param match_json: JSON decoded HTTP body identifying the request(s) to match. Must be JSON encodable.
        :param match_data: Multipart data (excluding files) identifying the request(s) to match. Must be a dictionary.
        :param match_files: Multipart files identifying the request(s) to match. Refer to httpx documentation for more information on supported values: https://www.python-httpx.org/advanced/clients/#multipart-file-encoding
        :param match_extensions: Extensions identifying the request(s) to match. Must be a dictionary.
        :param match_params: Query string parameters identifying the request(s) to match (if not provided as part of URL already). Must be a dictionary with str keys (parameter name) and str values (or a list of str values if parameter is provided more than once).
        :param is_optional: True will mark this callback as optional, False will expect a request matching it. Must be a boolean. Default to the opposite of assert_all_responses_were_requested option value (itself defaulting to True, meaning this parameter default to False).
        :param is_reusable: True will allow re-using this callback even if it already matched, False prevent re-using it. Must be a boolean. Default to the can_send_already_matched_responses option value (itself defaulting to False).
        """
        self._callbacks.append((_RequestMatcher(self._options, **matchers), callback))

    def add_exception(self, exception: BaseException, **matchers: Any) -> None:
        """
        Raise an exception if a request match.

        :param exception: The exception that will be raised upon reception of the matched request.
        :param url: Full URL identifying the request(s) to match. Use in addition to match_params if you do not want to provide query parameters as part of the URL.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param method: HTTP method identifying the request(s) to match.
        :param proxy_url: Full proxy URL identifying the request(s) to match.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param match_headers: HTTP headers identifying the request(s) to match. Must be a dictionary.
        :param match_content: Full HTTP body identifying the request(s) to match. Must be bytes.
        :param match_json: JSON decoded HTTP body identifying the request(s) to match. Must be JSON encodable.
        :param match_data: Multipart data (excluding files) identifying the request(s) to match. Must be a dictionary.
        :param match_files: Multipart files identifying the request(s) to match. Refer to httpx documentation for more information on supported values: https://www.python-httpx.org/advanced/clients/#multipart-file-encoding
        :param match_extensions: Extensions identifying the request(s) to match. Must be a dictionary.
        :param match_params: Query string parameters identifying the request(s) to match (if not provided as part of URL already). Must be a dictionary with str keys (parameter name) and str values (or a list of str values if parameter is provided more than once).
        :param is_optional: True will mark this exception response as optional, False will expect a request matching it. Must be a boolean. Default to the opposite of assert_all_responses_were_requested option value (itself defaulting to True, meaning this parameter default to False).
        :param is_reusable: True will allow re-using this exception response even if it already matched, False prevent re-using it. Must be a boolean. Default to the can_send_already_matched_responses option value (itself defaulting to False).
        """

        def exception_callback(request: httpx.Request) -> None:
            if isinstance(exception, httpx.RequestError):
                exception.request = request
            raise exception

        self.add_callback(exception_callback, **matchers)

    def _handle_request(
        self,
        real_transport: httpx.HTTPTransport,
        request: httpx.Request,
    ) -> httpx.Response:
        # Store the content in request for future matching
        request.read()
        self._requests.append((real_transport, request))

        callback = self._get_callback(real_transport, request)
        if callback:
            response = callback(request)

            if response:
                return _unread(response)

        self._request_not_matched(real_transport, request)

    async def _handle_async_request(
        self,
        real_transport: httpx.AsyncHTTPTransport,
        request: httpx.Request,
    ) -> httpx.Response:
        # Store the content in request for future matching
        await request.aread()
        self._requests.append((real_transport, request))

        callback = self._get_callback(real_transport, request)
        if callback:
            response = callback(request)

            if response:
                if inspect.isawaitable(response):
                    response = await response
                return _unread(response)

        self._request_not_matched(real_transport, request)

    def _request_not_matched(
        self,
        real_transport: Union[httpx.AsyncHTTPTransport, httpx.HTTPTransport],
        request: httpx.Request,
    ) -> NoReturn:
        self._requests_not_matched.append(request)
        raise httpx.TimeoutException(
            self._explain_that_no_response_was_found(real_transport, request),
            request=request,
        )

    def _explain_that_no_response_was_found(
        self,
        real_transport: Union[httpx.BaseTransport, httpx.AsyncBaseTransport],
        request: httpx.Request,
    ) -> str:
        matchers = [matcher for matcher, _ in self._callbacks]

        message = f"No response can be found for {RequestDescription(real_transport, request, matchers)}"

        already_matched = []
        unmatched = []
        for matcher in matchers:
            if matcher.nb_calls:
                already_matched.append(matcher)
            else:
                unmatched.append(matcher)

        matchers_description = "\n".join(
            [f"- {matcher}" for matcher in unmatched + already_matched]
        )
        if matchers_description:
            message += f" amongst:\n{matchers_description}"
            # If we could not find a response, but we have already matched responses
            # it might be that user is expecting one of those responses to be reused
            if any(not matcher.is_reusable for matcher in already_matched):
                message += "\n\nIf you wanted to reuse an already matched response instead of registering it again, refer to https://github.com/Colin-b/pytest_httpx/blob/master/README.md#allow-to-register-a-response-for-more-than-one-request"

        return message

    def _get_callback(
        self,
        real_transport: Union[httpx.HTTPTransport, httpx.AsyncHTTPTransport],
        request: httpx.Request,
    ) -> Optional[
        Callable[
            [httpx.Request],
            Union[Optional[httpx.Response], Awaitable[Optional[httpx.Response]]],
        ]
    ]:
        callbacks = [
            (matcher, callback)
            for matcher, callback in self._callbacks
            if matcher.match(real_transport, request)
        ]

        # No callback match this request
        if not callbacks:
            return None

        # Callbacks match this request
        for matcher, callback in callbacks:
            # Return the first not yet called
            if not matcher.nb_calls:
                matcher.nb_calls += 1
                return callback

        # Or the last registered (if it can be reused)
        if matcher.is_reusable:
            matcher.nb_calls += 1
            return callback

        # All callbacks have already been matched and last registered cannot be reused
        return None

    def get_requests(self, **matchers: Any) -> list[httpx.Request]:
        """
        Return all requests sent that match (empty list if no requests were matched).

        :param url: Full URL identifying the requests to retrieve. Use in addition to match_params if you do not want to provide query parameters as part of the URL.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param method: HTTP method identifying the requests to retrieve. Must be an upper-cased string value.
        :param proxy_url: Full proxy URL identifying the requests to retrieve.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param match_headers: HTTP headers identifying the requests to retrieve. Must be a dictionary.
        :param match_content: Full HTTP body identifying the requests to retrieve. Must be bytes.
        :param match_json: JSON decoded HTTP body identifying the requests to retrieve. Must be JSON encodable.
        :param match_data: Multipart data (excluding files) identifying the requests to retrieve. Must be a dictionary.
        :param match_files: Multipart files identifying the requests to retrieve. Refer to httpx documentation for more information on supported values: https://www.python-httpx.org/advanced/clients/#multipart-file-encoding
        :param match_extensions: Extensions identifying the requests to retrieve. Must be a dictionary.
        :param match_params: Query string parameters identifying the requests to retrieve (if not provided as part of URL already). Must be a dictionary with str keys (parameter name) and str values (or a list of str values if parameter is provided more than once).
        """
        matcher = _RequestMatcher(self._options, **matchers)
        return [
            request
            for real_transport, request in self._requests
            if matcher.match(real_transport, request)
        ]

    def get_request(self, **matchers: Any) -> Optional[httpx.Request]:
        """
        Return the single request that match (or None).

        :param url: Full URL identifying the request to retrieve. Use in addition to match_params if you do not want to provide query parameters as part of the URL.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param method: HTTP method identifying the request to retrieve. Must be an upper-cased string value.
        :param proxy_url: Full proxy URL identifying the request to retrieve.
        Can be a str, a re.Pattern instance or a httpx.URL instance.
        :param match_headers: HTTP headers identifying the request to retrieve. Must be a dictionary.
        :param match_content: Full HTTP body identifying the request to retrieve. Must be bytes.
        :param match_json: JSON decoded HTTP body identifying the request to retrieve. Must be JSON encodable.
        :param match_data: Multipart data (excluding files) identifying the request to retrieve. Must be a dictionary.
        :param match_files: Multipart files identifying the request to retrieve. Refer to httpx documentation for more information on supported values: https://www.python-httpx.org/advanced/clients/#multipart-file-encoding
        :param match_extensions: Extensions identifying the request to retrieve. Must be a dictionary.
        :param match_params: Query string parameters identifying the request to retrieve (if not provided as part of URL already). Must be a dictionary with str keys (parameter name) and str values (or a list of str values if parameter is provided more than once).
        :raises AssertionError: in case more than one request match.
        """
        requests = self.get_requests(**matchers)
        assert (
            len(requests) <= 1
        ), f"More than one request ({len(requests)}) matched, use get_requests instead or refine your filters."
        return requests[0] if requests else None

    def reset(self) -> None:
        self._requests.clear()
        self._callbacks.clear()
        self._requests_not_matched.clear()

    def _assert_options(self) -> None:
        callbacks_not_executed = [
            matcher for matcher, _ in self._callbacks if matcher.should_have_matched()
        ]
        matchers_description = "\n".join(
            [f"- {matcher}" for matcher in callbacks_not_executed]
        )

        assert not callbacks_not_executed, (
            "The following responses are mocked but not requested:\n"
            f"{matchers_description}\n"
            "\n"
            "If this is on purpose, refer to https://github.com/Colin-b/pytest_httpx/blob/master/README.md#allow-to-register-more-responses-than-what-will-be-requested"
        )

        if self._options.assert_all_requests_were_expected:
            requests_description = "\n".join(
                [
                    f"- {request.method} request on {request.url}"
                    for request in self._requests_not_matched
                ]
            )
            assert not self._requests_not_matched, (
                f"The following requests were not expected:\n"
                f"{requests_description}\n"
                "\n"
                "If this is on purpose, refer to https://github.com/Colin-b/pytest_httpx/blob/master/README.md#allow-to-not-register-responses-for-every-request"
            )


def _unread(response: httpx.Response) -> httpx.Response:
    # Allow to read the response on client side
    response.is_stream_consumed = False
    response.is_closed = False
    if hasattr(response, "_content"):
        del response._content
    return response
