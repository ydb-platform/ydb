from __future__ import absolute_import, print_function, division, unicode_literals

import _io
import inspect
import json as json_module
import logging
import re
from itertools import groupby

import six

from collections import namedtuple
from functools import update_wrapper
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError
from requests.utils import cookiejar_from_dict
from responses.matchers import json_params_matcher as _json_params_matcher
from responses.matchers import urlencoded_params_matcher as _urlencoded_params_matcher
from responses.registries import FirstMatchRegistry
from responses.matchers import query_string_matcher as _query_string_matcher
from warnings import warn

try:
    from collections.abc import Sequence, Sized
except ImportError:
    from collections import Sequence, Sized

try:
    from requests.packages.urllib3.response import HTTPResponse
except ImportError:  # pragma: no cover
    from urllib3.response import HTTPResponse  # pragma: no cover
try:
    from requests.packages.urllib3.connection import HTTPHeaderDict
except ImportError:  # pragma: no cover
    from urllib3.response import HTTPHeaderDict  # pragma: no cover
try:
    from requests.packages.urllib3.util.url import parse_url
except ImportError:  # pragma: no cover
    from urllib3.util.url import parse_url  # pragma: no cover

if six.PY2:
    from urlparse import urlparse, urlunparse, parse_qsl, urlsplit, urlunsplit
    from urllib import quote
else:
    from urllib.parse import (
        urlparse,
        urlunparse,
        parse_qsl,
        urlsplit,
        urlunsplit,
        quote,
    )

if six.PY2:
    try:
        from six import cStringIO as BufferIO
    except ImportError:
        from six import StringIO as BufferIO
else:
    from io import BytesIO as BufferIO

try:
    from unittest import mock as std_mock
except ImportError:
    import mock as std_mock

try:
    Pattern = re._pattern_type
except AttributeError:
    # Python 3.7
    Pattern = re.Pattern

UNSET = object()

Call = namedtuple("Call", ["request", "response"])

_real_send = HTTPAdapter.send

logger = logging.getLogger("responses")

if six.PY2:
    warn(
        "Support for Python 2.7 is being removed from the next release of responses. "
        "Pin your dependency to responses==0.17 or lower.",
        DeprecationWarning,
    )


def urlencoded_params_matcher(params):
    warn(
        "Function is deprecated. Use 'from responses.matchers import urlencoded_params_matcher'",
        DeprecationWarning,
    )
    return _urlencoded_params_matcher(params)


def json_params_matcher(params):
    warn(
        "Function is deprecated. Use 'from responses.matchers import json_params_matcher'",
        DeprecationWarning,
    )
    return _json_params_matcher(params)


def _is_string(s):
    return isinstance(s, six.string_types)


def _has_unicode(s):
    return any(ord(char) > 128 for char in s)


def _clean_unicode(url):
    # Clean up domain names, which use punycode to handle unicode chars
    urllist = list(urlsplit(url))
    netloc = urllist[1]
    if _has_unicode(netloc):
        domains = netloc.split(".")
        for i, d in enumerate(domains):
            if _has_unicode(d):
                d = "xn--" + d.encode("punycode").decode("ascii")
                domains[i] = d
        urllist[1] = ".".join(domains)
        url = urlunsplit(urllist)

    # Clean up path/query/params, which use url-encoding to handle unicode chars
    if isinstance(url.encode("utf8"), six.string_types):
        url = url.encode("utf8")
    chars = list(url)
    for i, x in enumerate(chars):
        if ord(x) > 128:
            chars[i] = quote(x)

    return "".join(chars)


def _ensure_str(s):
    if six.PY2:
        s = s.encode("utf-8") if isinstance(s, six.text_type) else s
    return s


def _cookies_from_headers(headers):
    try:
        import http.cookies as _cookies

        resp_cookie = _cookies.SimpleCookie()
        resp_cookie.load(headers["set-cookie"])

        cookies_dict = {name: v.value for name, v in resp_cookie.items()}
    except (ImportError, AttributeError):
        from cookies import Cookies

        resp_cookies = Cookies.from_request(_ensure_str(headers["set-cookie"]))
        cookies_dict = {
            v.name: quote(_ensure_str(v.value)) for _, v in resp_cookies.items()
        }
    return cookiejar_from_dict(cookies_dict)


_wrapper_template = """\
def wrapper%(wrapper_args)s:
    with responses:
        return func%(func_args)s
"""


def get_wrapped(func, responses, registry=None):
    if six.PY2:
        args, a, kw, defaults = inspect.getargspec(func)
        wrapper_args = inspect.formatargspec(args, a, kw, defaults)

        # Preserve the argspec for the wrapped function so that testing
        # tools such as pytest can continue to use their fixture injection.
        func_args = inspect.formatargspec(args, a, kw, None)
    else:
        signature = inspect.signature(func)
        signature = signature.replace(return_annotation=inspect.Signature.empty)
        # If the function is wrapped, switch to *args, **kwargs for the parameters
        # as we can't rely on the signature to give us the arguments the function will
        # be called with. For example unittest.mock.patch uses required args that are
        # not actually passed to the function when invoked.
        if hasattr(func, "__wrapped__"):
            wrapper_params = [
                inspect.Parameter("args", inspect.Parameter.VAR_POSITIONAL),
                inspect.Parameter("kwargs", inspect.Parameter.VAR_KEYWORD),
            ]
        else:
            wrapper_params = [
                param.replace(annotation=inspect.Parameter.empty)
                for param in signature.parameters.values()
            ]
        signature = signature.replace(parameters=wrapper_params)

        wrapper_args = str(signature)
        params_without_defaults = [
            param.replace(
                annotation=inspect.Parameter.empty, default=inspect.Parameter.empty
            )
            for param in signature.parameters.values()
        ]
        signature = signature.replace(parameters=params_without_defaults)
        func_args = str(signature)

    if registry is not None:
        responses._set_registry(registry)

    evaldict = {"func": func, "responses": responses}
    six.exec_(
        _wrapper_template % {"wrapper_args": wrapper_args, "func_args": func_args},
        evaldict,
    )
    wrapper = evaldict["wrapper"]
    update_wrapper(wrapper, func)
    return wrapper


class CallList(Sequence, Sized):
    def __init__(self):
        self._calls = []

    def __iter__(self):
        return iter(self._calls)

    def __len__(self):
        return len(self._calls)

    def __getitem__(self, idx):
        return self._calls[idx]

    def add(self, request, response):
        self._calls.append(Call(request, response))

    def reset(self):
        self._calls = []


def _ensure_url_default_path(url):
    if _is_string(url):
        url_parts = list(urlsplit(url))
        if url_parts[2] == "":
            url_parts[2] = "/"
        url = urlunsplit(url_parts)
    return url


def _get_url_and_path(url):
    url_parsed = urlparse(url)
    url_and_path = urlunparse(
        [url_parsed.scheme, url_parsed.netloc, url_parsed.path, None, None, None]
    )
    return parse_url(url_and_path).url


def _handle_body(body):
    if isinstance(body, six.text_type):
        body = body.encode("utf-8")
    if isinstance(body, _io.BufferedReader):
        return body

    data = BufferIO(body)

    def is_closed():
        """
        Real Response uses HTTPResponse as body object.
        Thus, when method is_closed is called first to check if there is any more
        content to consume and the file-like object is still opened

        This method ensures stability to work for both:
        https://github.com/getsentry/responses/issues/438
        https://github.com/getsentry/responses/issues/394

        where file should be intentionally be left opened to continue consumption
        """
        if not data.closed and data.read(1):
            # if there is more bytes to read then keep open, but return pointer
            data.seek(-1, 1)
            return False
        else:
            if not data.closed:
                # close but return False to mock like is still opened
                data.close()
                return False

            # only if file really closed (by us) return True
            return True

    data.isclosed = is_closed
    return data


class BaseResponse(object):
    passthrough = False
    content_type = None
    headers = None

    stream = False

    def __init__(self, method, url, match_querystring=None, match=()):
        self.method = method
        # ensure the url has a default path set if the url is a string
        self.url = _ensure_url_default_path(url)

        if self._should_match_querystring(match_querystring):
            match = tuple(match) + (_query_string_matcher(urlparse(self.url).query),)

        self.match = match
        self.call_count = 0

    def __eq__(self, other):
        if not isinstance(other, BaseResponse):
            return False

        if self.method != other.method:
            return False

        # Can't simply do an equality check on the objects directly here since __eq__ isn't
        # implemented for regex. It might seem to work as regex is using a cache to return
        # the same regex instances, but it doesn't in all cases.
        self_url = self.url.pattern if isinstance(self.url, Pattern) else self.url
        other_url = other.url.pattern if isinstance(other.url, Pattern) else other.url

        return self_url == other_url

    def __ne__(self, other):
        return not self.__eq__(other)

    def _should_match_querystring(self, match_querystring_argument):
        if isinstance(self.url, Pattern):
            # the old default from <= 0.9.0
            return False

        if match_querystring_argument is not None:
            warn(
                (
                    "Argument 'match_querystring' is deprecated. "
                    "Use 'responses.matchers.query_param_matcher' or "
                    "'responses.matchers.query_string_matcher'"
                ),
                DeprecationWarning,
            )
            return match_querystring_argument

        return bool(urlparse(self.url).query)

    def _url_matches(self, url, other):
        if _is_string(url):
            if _has_unicode(url):
                url = _clean_unicode(url)
                if not isinstance(other, six.text_type):
                    other = other.encode("ascii").decode("utf8")

            return _get_url_and_path(url) == _get_url_and_path(other)

        elif isinstance(url, Pattern) and url.match(other):
            return True

        else:
            return False

    @staticmethod
    def _req_attr_matches(match, request):
        for matcher in match:
            valid, reason = matcher(request)
            if not valid:
                return False, reason

        return True, ""

    def get_headers(self):
        headers = HTTPHeaderDict()  # Duplicate headers are legal
        if self.content_type is not None:
            headers["Content-Type"] = self.content_type
        if self.headers:
            headers.extend(self.headers)
        return headers

    def get_response(self, request):
        raise NotImplementedError

    def matches(self, request):
        if request.method != self.method:
            return False, "Method does not match"

        if not self._url_matches(self.url, request.url):
            return False, "URL does not match"

        valid, reason = self._req_attr_matches(self.match, request)
        if not valid:
            return False, reason

        return True, ""


class Response(BaseResponse):
    def __init__(
        self,
        method,
        url,
        body="",
        json=None,
        status=200,
        headers=None,
        stream=None,
        content_type=UNSET,
        auto_calculate_content_length=False,
        **kwargs
    ):
        # if we were passed a `json` argument,
        # override the body and content_type
        if json is not None:
            assert not body
            body = json_module.dumps(json)
            if content_type is UNSET:
                content_type = "application/json"

        if content_type is UNSET:
            if isinstance(body, six.text_type) and _has_unicode(body):
                content_type = "text/plain; charset=utf-8"
            else:
                content_type = "text/plain"

        self.body = body
        self.status = status
        self.headers = headers

        if stream is not None:
            warn(
                "stream argument is deprecated. Use stream parameter in request directly",
                DeprecationWarning,
            )

        self.stream = stream
        self.content_type = content_type
        self.auto_calculate_content_length = auto_calculate_content_length
        super(Response, self).__init__(method, url, **kwargs)

    def get_response(self, request):
        if self.body and isinstance(self.body, Exception):
            raise self.body

        headers = self.get_headers()
        status = self.status
        body = _handle_body(self.body)

        if (
            self.auto_calculate_content_length
            and isinstance(body, BufferIO)
            and "Content-Length" not in headers
        ):
            content_length = len(body.getvalue())
            headers["Content-Length"] = str(content_length)

        return HTTPResponse(
            status=status,
            reason=six.moves.http_client.responses.get(status),
            body=body,
            headers=headers,
            original_response=OriginalResponseShim(headers),
            preload_content=False,
        )

    def __repr__(self):
        return (
            "<Response(url='{url}' status={status} "
            "content_type='{content_type}' headers='{headers}')>".format(
                url=self.url,
                status=self.status,
                content_type=self.content_type,
                headers=json_module.dumps(self.headers),
            )
        )


class CallbackResponse(BaseResponse):
    def __init__(
        self, method, url, callback, stream=None, content_type="text/plain", **kwargs
    ):
        self.callback = callback

        if stream is not None:
            warn(
                "stream argument is deprecated. Use stream parameter in request directly",
                DeprecationWarning,
            )
        self.stream = stream
        self.content_type = content_type
        super(CallbackResponse, self).__init__(method, url, **kwargs)

    def get_response(self, request):
        headers = self.get_headers()

        result = self.callback(request)
        if isinstance(result, Exception):
            raise result

        status, r_headers, body = result
        if isinstance(body, Exception):
            raise body

        # If the callback set a content-type remove the one
        # set in add_callback() so that we don't have multiple
        # content type values.
        has_content_type = False
        if isinstance(r_headers, dict) and "Content-Type" in r_headers:
            has_content_type = True
        elif isinstance(r_headers, list):
            has_content_type = any(
                [h for h in r_headers if h and h[0].lower() == "content-type"]
            )
        if has_content_type:
            headers.pop("Content-Type", None)

        body = _handle_body(body)
        headers.extend(r_headers)

        return HTTPResponse(
            status=status,
            reason=six.moves.http_client.responses.get(status),
            body=body,
            headers=headers,
            original_response=OriginalResponseShim(headers),
            preload_content=False,
        )


class PassthroughResponse(BaseResponse):
    passthrough = True


class OriginalResponseShim(object):
    """
    Shim for compatibility with older versions of urllib3

    requests cookie handling depends on responses having a property chain of
    `response._original_response.msg` which contains the response headers [1]

    Using HTTPResponse() for this purpose causes compatibility errors with
    urllib3<1.23.0. To avoid adding more dependencies we can use this shim.

    [1]: https://github.com/psf/requests/blob/75bdc998e2d/requests/cookies.py#L125
    """

    def __init__(self, headers):
        self.msg = headers

    def isclosed(self):
        return True

    def close(self):
        return


class RequestsMock(object):
    DELETE = "DELETE"
    GET = "GET"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"
    PATCH = "PATCH"
    POST = "POST"
    PUT = "PUT"
    response_callback = None

    def __init__(
        self,
        assert_all_requests_are_fired=True,
        response_callback=None,
        passthru_prefixes=(),
        target="requests.adapters.HTTPAdapter.send",
        registry=FirstMatchRegistry,
    ):
        self._calls = CallList()
        self.reset()
        self._registry = registry()  # call only after reset
        self.assert_all_requests_are_fired = assert_all_requests_are_fired
        self.response_callback = response_callback
        self.passthru_prefixes = tuple(passthru_prefixes)
        self.target = target
        self._patcher = None
        self._matches = []

    def _get_registry(self):
        return self._registry

    def _set_registry(self, new_registry):
        if self.registered():
            err_msg = (
                "Cannot replace Registry, current registry has responses.\n"
                "Run 'responses.registry.reset()' first"
            )
            raise AttributeError(err_msg)

        self._registry = new_registry()

    def reset(self):
        self._registry = FirstMatchRegistry()
        self._calls.reset()
        self.passthru_prefixes = ()

    def add(
        self,
        method=None,  # method or ``Response``
        url=None,
        body="",
        adding_headers=None,
        *args,
        **kwargs
    ):
        """
        >>> import responses

        A basic request:
        >>> responses.add(responses.GET, 'http://example.com')

        You can also directly pass an object which implements the
        ``BaseResponse`` interface:

        >>> responses.add(Response(...))

        A JSON payload:

        >>> responses.add(
        >>>     method='GET',
        >>>     url='http://example.com',
        >>>     json={'foo': 'bar'},
        >>> )

        Custom headers:

        >>> responses.add(
        >>>     method='GET',
        >>>     url='http://example.com',
        >>>     headers={'X-Header': 'foo'},
        >>> )

        """
        if isinstance(method, BaseResponse):
            self._registry.add(method)
            return

        if adding_headers is not None:
            kwargs.setdefault("headers", adding_headers)

        self._registry.add(Response(method=method, url=url, body=body, **kwargs))

    def add_passthru(self, prefix):
        """
        Register a URL prefix or regex to passthru any non-matching mock requests to.

        For example, to allow any request to 'https://example.com', but require
        mocks for the remainder, you would add the prefix as so:

        >>> import responses
        >>> responses.add_passthru('https://example.com')

        Regex can be used like:

        >>> responses.add_passthru(re.compile('https://example.com/\\w+'))
        """
        if not isinstance(prefix, Pattern) and _has_unicode(prefix):
            prefix = _clean_unicode(prefix)
        self.passthru_prefixes += (prefix,)

    def remove(self, method_or_response=None, url=None):
        """
        Removes a response previously added using ``add()``, identified
        either by a response object inheriting ``BaseResponse`` or
        ``method`` and ``url``. Removes all matching responses.

        >>> import responses
        >>> responses.add(responses.GET, 'http://example.org')
        >>> responses.remove(responses.GET, 'http://example.org')
        """
        if isinstance(method_or_response, BaseResponse):
            response = method_or_response
        else:
            response = BaseResponse(method=method_or_response, url=url)

        self._registry.remove(response)

    def replace(self, method_or_response=None, url=None, body="", *args, **kwargs):
        """
        Replaces a response previously added using ``add()``. The signature
        is identical to ``add()``. The response is identified using ``method``
        and ``url``, and the first matching response is replaced.

        >>> import responses
        >>> responses.add(responses.GET, 'http://example.org', json={'data': 1})
        >>> responses.replace(responses.GET, 'http://example.org', json={'data': 2})
        """
        if isinstance(method_or_response, BaseResponse):
            url = method_or_response.url
            response = method_or_response
        else:
            response = Response(method=method_or_response, url=url, body=body, **kwargs)

        self._registry.replace(response)

    def upsert(self, method_or_response=None, url=None, body="", *args, **kwargs):
        """
        Replaces a response previously added using ``add()``, or adds the response
        if no response exists.  Responses are matched using ``method``and ``url``.
        The first matching response is replaced.

        >>> import responses
        >>> responses.add(responses.GET, 'http://example.org', json={'data': 1})
        >>> responses.upsert(responses.GET, 'http://example.org', json={'data': 2})
        """
        try:
            self.replace(method_or_response, url, body, *args, **kwargs)
        except ValueError:
            self.add(method_or_response, url, body, *args, **kwargs)

    def add_callback(
        self,
        method,
        url,
        callback,
        match_querystring=False,
        content_type="text/plain",
        match=(),
    ):
        # ensure the url has a default path set if the url is a string
        # url = _ensure_url_default_path(url, match_querystring)

        self._registry.add(
            CallbackResponse(
                url=url,
                method=method,
                callback=callback,
                content_type=content_type,
                match_querystring=match_querystring,
                match=match,
            )
        )

    def registered(self):
        return self._registry.registered

    @property
    def calls(self):
        return self._calls

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        success = type is None
        self.stop(allow_assert=success)
        self.reset()
        return success

    def activate(self, func=None, registry=None):
        if func is not None:
            return get_wrapped(func, self)

        def deco_activate(func):
            return get_wrapped(func, self, registry)

        return deco_activate

    def _find_match(self, request):
        """
        Iterates through all available matches and validates if any of them matches the request

        :param request: (PreparedRequest), request object
        :return:
            (Response) found match. If multiple found, then remove & return the first match.
            (list) list with reasons why other matches don't match
        """
        return self._registry.find(request)

    def _parse_request_params(self, url):
        params = {}
        for key, val in groupby(parse_qsl(urlparse(url).query), lambda kv: kv[0]):
            values = list(map(lambda x: x[1], val))
            if len(values) == 1:
                values = values[0]
            params[key] = values
        return params

    def _on_request(self, adapter, request, **kwargs):
        # add attributes params and req_kwargs to 'request' object for further match comparison
        # original request object does not have these attributes
        request.params = self._parse_request_params(request.path_url)
        request.req_kwargs = kwargs

        match, match_failed_reasons = self._find_match(request)
        resp_callback = self.response_callback

        if match is None:
            if any(
                [
                    p.match(request.url)
                    if isinstance(p, Pattern)
                    else request.url.startswith(p)
                    for p in self.passthru_prefixes
                ]
            ):
                logger.info("request.allowed-passthru", extra={"url": request.url})
                return _real_send(adapter, request, **kwargs)

            error_msg = (
                "Connection refused by Responses - the call doesn't "
                "match any registered mock.\n\n"
                "Request: \n"
                "- %s %s\n\n"
                "Available matches:\n" % (request.method, request.url)
            )
            for i, m in enumerate(self.registered()):
                error_msg += "- {} {} {}\n".format(
                    m.method, m.url, match_failed_reasons[i]
                )

            response = ConnectionError(error_msg)
            response.request = request

            self._calls.add(request, response)
            response = resp_callback(response) if resp_callback else response
            raise response

        if match.passthrough:
            logger.info("request.passthrough-response", extra={"url": request.url})
            response = _real_send(adapter, request, **kwargs)
        else:
            try:
                response = adapter.build_response(request, match.get_response(request))
            except BaseException as response:
                match.call_count += 1
                self._calls.add(request, response)
                response = resp_callback(response) if resp_callback else response
                raise

        response = resp_callback(response) if resp_callback else response
        match.call_count += 1
        self._calls.add(request, response)
        return response

    def start(self):
        def unbound_on_send(adapter, request, *a, **kwargs):
            return self._on_request(adapter, request, *a, **kwargs)

        self._patcher = std_mock.patch(target=self.target, new=unbound_on_send)
        self._patcher.start()

    def stop(self, allow_assert=True):
        self._patcher.stop()
        if not self.assert_all_requests_are_fired:
            return

        if not allow_assert:
            return

        not_called = [m for m in self.registered() if m.call_count == 0]
        if not_called:
            raise AssertionError(
                "Not all requests have been executed {0!r}".format(
                    [(match.method, match.url) for match in not_called]
                )
            )

    def assert_call_count(self, url, count):
        call_count = len(
            [
                1
                for call in self.calls
                if call.request.url == _ensure_url_default_path(url)
            ]
        )
        if call_count == count:
            return True
        else:
            raise AssertionError(
                "Expected URL '{0}' to be called {1} times. Called {2} times.".format(
                    url, count, call_count
                )
            )


# expose default mock namespace
mock = _default_mock = RequestsMock(assert_all_requests_are_fired=False)
__all__ = [
    "CallbackResponse",
    "Response",
    "RequestsMock",
    # Exposed by the RequestsMock class:
    "activate",
    "add",
    "add_callback",
    "add_passthru",
    "assert_all_requests_are_fired",
    "assert_call_count",
    "calls",
    "DELETE",
    "GET",
    "HEAD",
    "OPTIONS",
    "passthru_prefixes",
    "PATCH",
    "POST",
    "PUT",
    "registered",
    "remove",
    "replace",
    "reset",
    "response_callback",
    "start",
    "stop",
    "target",
    "upsert",
]

activate = _default_mock.activate
add = _default_mock.add
add_callback = _default_mock.add_callback
add_passthru = _default_mock.add_passthru
assert_all_requests_are_fired = _default_mock.assert_all_requests_are_fired
assert_call_count = _default_mock.assert_call_count
calls = _default_mock.calls
DELETE = _default_mock.DELETE
GET = _default_mock.GET
HEAD = _default_mock.HEAD
OPTIONS = _default_mock.OPTIONS
passthru_prefixes = _default_mock.passthru_prefixes
PATCH = _default_mock.PATCH
POST = _default_mock.POST
PUT = _default_mock.PUT
registered = _default_mock.registered
remove = _default_mock.remove
replace = _default_mock.replace
reset = _default_mock.reset
response_callback = _default_mock.response_callback
start = _default_mock.start
stop = _default_mock.stop
target = _default_mock.target
upsert = _default_mock.upsert
