# -*- coding: utf-8 -*-
import copy
import logging
import typing

import requests.auth
import requests.exceptions
import six
from bravado_core.operation import Operation
from bravado_core.response import IncomingResponse
from six import iteritems
from six.moves.urllib import parse as urlparse

from bravado._equality_util import are_objects_equal as _are_objects_equal
from bravado.config import RequestConfig
from bravado.http_client import HttpClient
from bravado.http_future import FutureAdapter
from bravado.http_future import HttpFuture


if getattr(typing, 'TYPE_CHECKING', False):
    T = typing.TypeVar('T')


log = logging.getLogger(__name__)


class Authenticator(object):
    """Authenticates requests.

    :param host: Host to authenticate for.
    """

    def __init__(self, host):
        # type: (str) -> None
        self.host = host

    def __repr__(self):
        # type: () -> str
        return "%s(%s)" % (self.__class__.__name__, self.host)

    def matches(self, url):
        # type: (typing.Text) -> bool
        """Returns true if this authenticator applies to the given url.

        :param url: URL to check.
        :return: True if matches host and port, False otherwise.
        """
        split = urlparse.urlsplit(url)
        return self.host == split.netloc

    def apply(self, request):
        # type: (requests.Request) -> requests.Request
        """Apply authentication to a request.

        :param request: Request to add authentication information to.
        """
        raise NotImplementedError(u"%s: Method not implemented",
                                  self.__class__.__name__)


# noinspection PyDocstring
class ApiKeyAuthenticator(Authenticator):
    """?api_key authenticator.

    This authenticator adds an API key via query parameter or header.

    :param host: Host to authenticate for.
    :param api_key: API key.
    :param param_name: Query parameter specifying the API key.
    :param param_in: How to send the API key. Can be 'query' or 'header'.
    """

    def __init__(
        self,
        host,  # type: str
        api_key,  # type: typing.Text
        param_name=u'api_key',  # type: typing.Text
        param_in=u'query',  # type: typing.Text
    ):
        # type: (...) -> None
        super(ApiKeyAuthenticator, self).__init__(host)
        self.param_name = param_name
        self.param_in = param_in
        self.api_key = api_key

    def apply(self, request):
        # type: (requests.Request) -> requests.Request
        if self.param_in == 'header':
            request.headers.setdefault(self.param_name, self.api_key)
        else:
            request.params[self.param_name] = self.api_key
        return request


class BasicAuthenticator(Authenticator):
    """HTTP Basic authenticator.

    :param host: Host to authenticate for.
    :param username: Username.
    :param password: Password
    """

    def __init__(
        self,
        host,  # type: str
        username,  # type: typing.Union[bytes, str]
        password,  # type: typing.Union[bytes, str]
    ):
        # type: (...) -> None
        super(BasicAuthenticator, self).__init__(host)
        self.auth = requests.auth.HTTPBasicAuth(username, password)

    def apply(self, request):
        # type: (requests.Request) -> requests.Request
        request.auth = self.auth

        return request


class RequestsResponseAdapter(IncomingResponse):
    """Wraps a requests.models.Response object to provide a uniform interface
    to the response innards.

    :type requests_lib_response: :class:`requests.models.Response`
    """

    def __init__(self, requests_lib_response):
        # type: (requests.Response) -> None
        self._delegate = requests_lib_response

    @property
    def status_code(self):
        # type: () -> int
        return self._delegate.status_code

    @property
    def text(self):
        # type: () -> typing.Text
        return self._delegate.text

    @property
    def raw_bytes(self):
        # type: () -> bytes
        return self._delegate.content

    @property
    def reason(self):
        # type: () -> typing.Text
        return self._delegate.reason

    @property
    def headers(self):
        # type: () -> typing.Mapping[typing.Text, typing.Text]
        # we don't use typing.cast here since that's broken on Python 3.5.1
        return self._delegate.headers  # type: ignore

    def json(self, **kwargs):
        # type: (typing.Any) -> typing.Mapping[typing.Text, typing.Any]
        return self._delegate.json(**kwargs)


class RequestsFutureAdapter(FutureAdapter):
    """Mimics a :class:`concurrent.futures.Future` for the purposes of making
    HTTP calls with the Requests library in a future-y sort of way.
    """

    timeout_errors = (requests.exceptions.ReadTimeout,)  # type: typing.Tuple[typing.Type[Exception], ...]
    connection_errors = (requests.exceptions.ConnectionError,)  # type: typing.Tuple[typing.Type[Exception], ...]

    def __init__(
        self,
        session,  # type: requests.Session
        request,  # type: requests.Request
        misc_options,  # type: typing.Mapping[str, typing.Any]
    ):
        # type: (...) -> None
        """Kicks API call for Requests client

        :param session: session object to use for making the request
        :param request: dict containing API request parameters
        :param misc_options: misc options to apply when sending a HTTP request.
            e.g. timeout, connect_timeout, etc
        :type misc_options: dict
        """
        self.session = session
        self.request = request
        self.misc_options = misc_options

    def build_timeout(
        self,
        result_timeout,  # type: typing.Optional[float]
    ):
        # type: (...) -> typing.Union[typing.Optional[float], typing.Tuple[typing.Optional[float], typing.Optional[float]]]  # noqa
        """
        Build the appropriate timeout object to pass to `session.send(...)`
        based on connect_timeout, the timeout passed to the service call, and
        the timeout passed to the result call.

        :param result_timeout: timeout that was passed into `future.result(..)`
        :return: timeout
        :rtype: float or tuple(connect_timeout, timeout)
        """
        # The API provides two ways to pass a timeout :( We're stuck
        # dealing with it until we're ready to make a non-backwards
        # compatible change.
        #
        #  - If both timeouts are the same, no problem
        #  - If either has a non-None value, use the non-None value
        #  - If both have a non-None value, use the greater of the two
        timeout = None
        has_service_timeout = 'timeout' in self.misc_options
        service_timeout = self.misc_options.get('timeout')

        if not has_service_timeout:
            timeout = result_timeout
        elif service_timeout == result_timeout:
            timeout = service_timeout
        else:
            if service_timeout is None:
                timeout = result_timeout
            elif result_timeout is None:
                timeout = service_timeout
            else:
                timeout = max(service_timeout, result_timeout)
                log.warning(
                    "Two different timeouts have been passed: "
                    "_request_options['timeout'] = %s and "
                    "future.result(timeout=%s). Using timeout of %s.",
                    service_timeout, result_timeout, timeout,
                )

        # Requests is weird in that if you want to specify a connect_timeout
        # and idle timeout, then the timeout is passed as a tuple
        if 'connect_timeout' in self.misc_options:
            return self.misc_options['connect_timeout'], timeout
        return timeout

    def result(self, timeout=None):
        # type: (typing.Optional[float]) -> requests.Response
        """Blocking call to wait for API response

        :param timeout: timeout in seconds to wait for response. Defaults to
            None to wait indefinitely.
        :type timeout: float
        :return: raw response from the server
        :rtype: dict
        """
        request = self.request

        # Ensure that all the headers are converted to strings.
        # This is need to workaround https://github.com/requests/requests/issues/3491
        request.headers = {
            k: str(v) if not isinstance(v, six.binary_type) else v
            for k, v in iteritems(request.headers)
        }

        prepared_request = self.session.prepare_request(request)
        settings = self.session.merge_environment_settings(
            prepared_request.url,
            proxies={},
            stream=None,
            verify=self.misc_options['ssl_verify'],
            cert=self.misc_options['ssl_cert'],
        )
        response = self.session.send(
            prepared_request,
            timeout=self.build_timeout(timeout),
            allow_redirects=self.misc_options['follow_redirects'],
            **settings
        )
        return response

    def cancel(self):
        # type: () -> None
        pass


class RequestsClient(HttpClient):
    """Synchronous HTTP client implementation.
    """

    def __init__(
        self,
        ssl_verify=True,  # type: bool
        ssl_cert=None,  # type:  typing.Any
        future_adapter_class=RequestsFutureAdapter,  # type: typing.Type[RequestsFutureAdapter]
        response_adapter_class=RequestsResponseAdapter,  # type: typing.Type[RequestsResponseAdapter]
    ):
        # type: (...) -> None
        """
        :param ssl_verify: Set to False to disable SSL certificate validation. Provide the path to a
            CA bundle if you need to use a custom one.
        :param ssl_cert: Provide a client-side certificate to use. Either a sequence of strings pointing
            to the certificate (1) and the private key (2), or a string pointing to the combined certificate
            and key.
        :param future_adapter_class: Custom future adapter class,
            should be a subclass of :class:`RequestsFutureAdapter`
        :param response_adapter_class: Custom response adapter class,
            should be a subclass of :class:`RequestsResponseAdapter`
        """
        self.session = requests.Session()
        self.authenticator = None  # type: typing.Optional[Authenticator]
        self.ssl_verify = ssl_verify
        self.ssl_cert = ssl_cert
        self.future_adapter_class = future_adapter_class
        self.response_adapter_class = response_adapter_class

    def __hash__(self):
        # type: () -> int
        return hash((
            self.session,
            self.authenticator,
            self.ssl_verify,
            self.ssl_cert,
            self.future_adapter_class,
            self.response_adapter_class,
        ))

    def __ne__(self, other):
        # type: (typing.Any) -> bool
        return not (self == other)

    def __eq__(self, other):
        # type: (typing.Any) -> bool
        return (
            _are_objects_equal(
                self, other,
                # requests.Session does not define equality methods
                attributes_to_ignore={'session'},
            ) and
            # We're checking for requests.Session to be mostly the same as custom
            # configurations (ie. max_redirects, proxies, SSL verification, etc.)
            # might be possible.
            _are_objects_equal(
                self.session, other.session,
                attributes_to_ignore={
                    'adapters',  # requests.adapters.HTTPAdapter do not define equality
                    'prefetch',  # attribute present in requests.Session.__attrs__ but never initialised or used
                },
            )
        )

    def separate_params(
        self,
        request_params,  # type: typing.MutableMapping[str, typing.Any]
    ):
        # type: (...) -> typing.Tuple[typing.Mapping[str, typing.Any], typing.Mapping[str, typing.Any]]
        """Splits the passed in dict of request_params into two buckets.

        - sanitized_params are valid kwargs for constructing a
          requests.Request(..)
        - misc_options are things like timeouts which can't be communicated
          to the Requests library via the requests.Request(...) constructor.

        :param request_params: kitchen sink of request params. Treated as a
            read-only dict.
        :returns: tuple(sanitized_params, misc_options)
        """
        sanitized_params = copy.copy(request_params)
        misc_options = {
            'ssl_verify': self.ssl_verify,
            'ssl_cert': self.ssl_cert,
            'follow_redirects': sanitized_params.pop('follow_redirects', False),
        }

        if 'connect_timeout' in sanitized_params:
            misc_options['connect_timeout'] = \
                sanitized_params.pop('connect_timeout')

        if 'timeout' in sanitized_params:
            misc_options['timeout'] = sanitized_params.pop('timeout')

        return sanitized_params, misc_options

    def request(
        self,
        request_params,  # type: typing.MutableMapping[str, typing.Any]
        operation=None,  # type: typing.Optional[Operation]
        request_config=None,  # type: typing.Optional[RequestConfig]
    ):
        # type: (...) -> HttpFuture[T]
        """
        :param request_params: complete request data.
        :type request_params: dict
        :param operation: operation that this http request is for. Defaults
            to None - in which case, we're obviously just retrieving a Swagger
            Spec.
        :type operation: :class:`bravado_core.operation.Operation`
        :param RequestConfig request_config: per-request configuration

        :returns: HTTP Future object
        :rtype: :class: `bravado_core.http_future.HttpFuture`
        """
        sanitized_params, misc_options = self.separate_params(request_params)

        requests_future = self.future_adapter_class(
            self.session,
            self.authenticated_request(sanitized_params),
            misc_options,
        )

        return HttpFuture(
            requests_future,
            self.response_adapter_class,
            operation,
            request_config,
        )

    def set_basic_auth(
        self,
        host,  # type: str
        username,  # type: typing.Union[bytes, str]
        password,  # type: typing.Union[bytes, str]
    ):
        # type: (...) -> None
        self.authenticator = BasicAuthenticator(
            host=host,
            username=username,
            password=password,
        )

    def set_api_key(
        self,
        host,  # type: str
        api_key,  # type: typing.Text
        param_name=u'api_key',  # type: typing.Text
        param_in=u'query',  # type: typing.Text
    ):
        # type: (...) -> None
        self.authenticator = ApiKeyAuthenticator(
            host=host,
            api_key=api_key,
            param_name=param_name,
            param_in=param_in,
        )

    def authenticated_request(self, request_params):
        # type: (typing.Mapping[str, typing.Any]) -> requests.Request
        return self.apply_authentication(requests.Request(**request_params))

    def apply_authentication(self, request):
        # type: (requests.Request) -> requests.Request
        if self.authenticator and self.authenticator.matches(request.url):
            return self.authenticator.apply(request)

        return request
