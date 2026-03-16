# -*- coding: utf-8 -*-
import logging
import sys
import traceback
import typing
from functools import wraps
from itertools import chain

import monotonic
import six
from bravado_core.content_type import APP_JSON
from bravado_core.content_type import APP_MSGPACK
from bravado_core.exception import MatchingResponseNotFound
from bravado_core.operation import Operation
from bravado_core.response import get_response_spec
from bravado_core.response import IncomingResponse
from bravado_core.unmarshal import unmarshal_schema_object
from bravado_core.validate import validate_schema_object
from msgpack import unpackb

from bravado.config import bravado_config_from_config_dict
from bravado.config import BravadoConfig
from bravado.config import CONFIG_DEFAULTS
from bravado.config import RequestConfig
from bravado.exception import BravadoConnectionError
from bravado.exception import BravadoTimeoutError
from bravado.exception import ForcedFallbackResultError
from bravado.exception import HTTPServerError
from bravado.exception import make_http_exception
from bravado.response import BravadoResponse


FuncType = typing.Callable[..., typing.Any]
F = typing.TypeVar('F', bound=FuncType)
T = typing.TypeVar('T')
log = logging.getLogger(__name__)


FALLBACK_EXCEPTIONS = (
    BravadoTimeoutError,
    BravadoConnectionError,
    HTTPServerError,
)


class _SENTINEL(object):
    pass


SENTINEL = _SENTINEL()


class FutureAdapter(typing.Generic[T]):
    """
    Mimics a :class:`concurrent.futures.Future` regardless of which client is
    performing the request, whether it is synchronous or actually asynchronous.

    This adapter must be implemented by all bravado clients such as FidoClient
    or RequestsClient to wrap the object returned by their 'request' method.

    """

    # Make sure to define the timeout errors associated with your http client
    timeout_errors = ()  # type: typing.Tuple[typing.Type[BaseException], ...]
    connection_errors = ()  # type: typing.Tuple[typing.Type[BaseException], ...]

    def _raise_error(self, base_exception_class, class_name_suffix, exception):
        # type: (typing.Type[BaseException], typing.Text, BaseException) -> typing.NoReturn
        error = type(
            '{}{}'.format(self.__class__.__name__, class_name_suffix),
            (exception.__class__, base_exception_class),
            dict(
                # Small hack to allow all exceptions to be generated even if they have parameters in the signature
                exception.__dict__,
                __init__=lambda *args, **kwargs: None,
            ),
        )()

        six.reraise(
            error.__class__,
            error,
            sys.exc_info()[2],
        )

    def _raise_timeout_error(self, exception):
        # type: (BaseException) -> typing.NoReturn
        self._raise_error(BravadoTimeoutError, 'Timeout', exception)

    def _raise_connection_error(self, exception):
        # type: (BaseException) -> typing.NoReturn
        self._raise_error(BravadoConnectionError, 'ConnectionError', exception)

    def result(self, timeout=None):
        # type: (typing.Optional[float]) -> T
        """
        Must implement a result method which blocks on result retrieval.

        :param timeout: maximum time to wait on result retrieval. Defaults to
            None which means blocking undefinitely.
        """
        raise NotImplementedError(
            "FutureAdapter must implement 'result' method"
        )

    def cancel(self):
        # type: () -> None
        """
        Must implement a cancel method which terminates any ongoing network
        request.
        """
        log.warning('The FutureAdapter class of the HTTP client does not implement cancel(), ignoring the call.')


def reraise_errors(func):
    # type: (F) -> F

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # type: (typing.Any, typing.Any, typing.Any) -> typing.Any
        timeout_errors = tuple(self.future.timeout_errors or ())
        connection_errors = tuple(self.future.connection_errors or ())

        try:
            return func(self, *args, **kwargs)
        except connection_errors as exception:
            self.future._raise_connection_error(exception)
        except timeout_errors as exception:
            self.future._raise_timeout_error(exception)

    return typing.cast(F, wrapper)


class HttpFuture(typing.Generic[T]):
    """Wrapper for a :class:`FutureAdapter` that returns an HTTP response.

    :param future: The future object to wrap.
    :type future: :class: `FutureAdapter`
    :param response_adapter: Adapter type which exposes the innards of the HTTP
        response in a non-http client specific way.
    :type response_adapter: type that is a subclass of
        :class:`bravado_core.response.IncomingResponse`.
    :param RequestConfig request_config: See :class:`bravado.config.RequestConfig` and
        :data:`bravado.client.REQUEST_OPTIONS_DEFAULTS`
    """

    def __init__(
        self,
        future,  # type: FutureAdapter
        response_adapter,  # type: typing.Callable[[typing.Any], IncomingResponse]
        operation=None,  # type: typing.Optional[Operation]
        request_config=None,  # type: typing.Optional[RequestConfig]
    ):
        # type: (...) -> None
        self._start_time = monotonic.monotonic()
        self.future = future
        self.response_adapter = response_adapter
        self.operation = operation
        self.request_config = request_config or RequestConfig(
            {},
            also_return_response_default=False,
        )

    @property
    def _bravado_config(self):
        # type: () -> BravadoConfig
        if self.operation:
            return self.operation.swagger_spec.config['bravado']
        else:
            return bravado_config_from_config_dict(CONFIG_DEFAULTS)

    def response(
        self,
        timeout=None,  # type: typing.Optional[float]
        fallback_result=SENTINEL,  # type: typing.Union[_SENTINEL, T, typing.Callable[[BaseException], T]]  # noqa
        exceptions_to_catch=FALLBACK_EXCEPTIONS,  # type: typing.Tuple[typing.Type[BaseException], ...]
    ):
        # type: (...) -> BravadoResponse[T]
        """Blocking call to wait for the HTTP response.

        :param timeout: Number of seconds to wait for a response. Defaults to
            None which means wait indefinitely.
        :type timeout: float
        :param fallback_result: either the swagger result or a callable that accepts an exception as argument
            and returns the swagger result to use in case of errors
        :param exceptions_to_catch: Exception classes to catch and call `fallback_result`
            with. Has no effect if `fallback_result` is not provided. By default, `fallback_result`
            will be called for read timeout and server errors (HTTP 5XX).
        :return: A BravadoResponse instance containing the swagger result and response metadata.
        """
        incoming_response = None
        exc_info = None  # type: typing.Optional[typing.List[typing.Union[typing.Type[BaseException], BaseException, typing.Text]]]  # noqa: E501
        request_end_time = None
        if self.request_config.force_fallback_result:
            exceptions_to_catch = tuple(chain(exceptions_to_catch, (ForcedFallbackResultError,)))

        try:
            incoming_response = self._get_incoming_response(timeout)
            request_end_time = monotonic.monotonic()

            swagger_result = self._get_swagger_result(incoming_response)

            if self.operation is None and incoming_response.status_code >= 300:
                raise make_http_exception(response=incoming_response)

            # Trigger fallback_result if the option is set
            if fallback_result is not SENTINEL and self.request_config.force_fallback_result:
                if self._bravado_config.disable_fallback_results:
                    log.warning(
                        'force_fallback_result set in request options and disable_fallback_results '
                        'set in client config; not using fallback result.'
                    )
                else:
                    # raise an exception to trigger fallback result handling
                    raise ForcedFallbackResultError()

        except exceptions_to_catch as e:
            if request_end_time is None:
                request_end_time = monotonic.monotonic()
            exc_info = []
            # the return values of exc_info are annotated as Optional, but we know they are set in this case.
            # additionally, we can't use a cast() since that caused a runtime exception on some older versions
            # of Python 3.5.
            exc_info.extend(sys.exc_info()[:2])  # type: ignore
            # the Python 2 documentation states that we shouldn't assign the traceback to a local variable,
            # as that would cause a circular reference. We'll store a string representation of the traceback
            # instead.
            exc_info.append(traceback.format_exc())

            if (
                fallback_result is not SENTINEL and
                self.operation and
                not self.operation.swagger_spec.config['bravado'].disable_fallback_results
            ):
                if callable(fallback_result):
                    swagger_result = fallback_result(e)
                else:
                    swagger_result = typing.cast(T, fallback_result)
            else:
                six.reraise(*sys.exc_info())

        metadata_class = self._bravado_config.response_metadata_class
        response_metadata = metadata_class(
            incoming_response=incoming_response,
            swagger_result=swagger_result,
            start_time=self._start_time,
            request_end_time=request_end_time,
            handled_exception_info=exc_info,
            request_config=self.request_config,
        )
        return BravadoResponse(
            result=swagger_result,
            metadata=response_metadata,
        )

    def result(
        self,
            timeout=None,  # type: typing.Optional[float]
    ):
        # type: (...) -> typing.Union[T, IncomingResponse, typing.Tuple[T, IncomingResponse]]
        """DEPRECATED: please use the `response()` method instead.

        Blocking call to wait for and return the unmarshalled swagger result.

        :param timeout: Number of seconds to wait for a response. Defaults to
            None which means wait indefinitely.
        :type timeout: float
        :return: Depends on the value of also_return_response sent in
            to the constructor.
        """
        incoming_response = self._get_incoming_response(timeout)
        swagger_result = self._get_swagger_result(incoming_response)

        if self.operation is not None:
            swagger_result = typing.cast(T, swagger_result)
            if self.request_config.also_return_response:
                return swagger_result, incoming_response
            return swagger_result

        if 200 <= incoming_response.status_code < 300:
            return incoming_response

        raise make_http_exception(response=incoming_response)

    def cancel(self):
        # type: () -> None
        return self.future.cancel()

    @reraise_errors
    def _get_incoming_response(self, timeout=None):
        # type: (typing.Optional[float]) -> IncomingResponse
        inner_response = self.future.result(timeout=timeout)
        incoming_response = self.response_adapter(inner_response)
        return incoming_response

    @reraise_errors  # unmarshal_response_inner calls response.json(), which might raise errors
    def _get_swagger_result(self, incoming_response):
        # type: (IncomingResponse) -> typing.Optional[T]
        swagger_result = None
        if self.operation is not None:
            unmarshal_response(
                incoming_response,
                self.operation,
                self.request_config.response_callbacks,
            )
            swagger_result = typing.cast(T, incoming_response.swagger_result)

        return swagger_result


def unmarshal_response(
    incoming_response,  # type: IncomingResponse
    operation,  # type: Operation
    response_callbacks=None,  # type: typing.Optional[typing.List[typing.Callable[[typing.Any, typing.Any], None]]]
):
    # type: (...) -> None
    """So the http_client is finished with its part of processing the response.
    This hands the response over to bravado_core for validation and
    unmarshalling and then runs any response callbacks. On success, the
    swagger_result is available as ``incoming_response.swagger_result``.
    :type incoming_response: :class:`bravado_core.response.IncomingResponse`
    :type operation: :class:`bravado_core.operation.Operation`
    :type response_callbacks: list of callable. See
        bravado_core.client.REQUEST_OPTIONS_DEFAULTS.
    :raises: HTTPError
        - On 5XX status code, the HTTPError has minimal information.
        - On non-2XX status code with no matching response, the HTTPError
            contains a detailed error message.
        - On non-2XX status code with a matching response, the HTTPError
            contains the return value.
    """
    response_callbacks = response_callbacks or []

    try:
        raise_on_unexpected(incoming_response)
        incoming_response.swagger_result = unmarshal_response_inner(  # type: ignore
            response=incoming_response,
            op=operation,
        )
    except MatchingResponseNotFound as e:
        exception = make_http_exception(
            response=incoming_response,
            message=str(e)
        )
        six.reraise(
            type(exception),
            exception,
            sys.exc_info()[2])
    finally:
        # Always run the callbacks regardless of success/failure
        for response_callback in response_callbacks:
            response_callback(incoming_response, operation)

    raise_on_expected(incoming_response)


def unmarshal_response_inner(
    response,  # type: IncomingResponse
    op,  # type: Operation
):
    # type: (...) -> typing.Optional[T]
    """
    Unmarshal incoming http response into a value based on the
    response specification.
    :type response: :class:`bravado_core.response.IncomingResponse`
    :type op: :class:`bravado_core.operation.Operation`
    :returns: value where type(value) matches response_spec['schema']['type']
        if it exists, None otherwise.
    """
    deref = op.swagger_spec.deref
    response_spec = get_response_spec(status_code=response.status_code, op=op)

    if 'schema' not in response_spec:
        return None

    content_type = response.headers.get('content-type', '').lower()

    if content_type.startswith(APP_JSON) or content_type.startswith(APP_MSGPACK):
        content_spec = deref(response_spec['schema'])
        if content_type.startswith(APP_JSON):
            content_value = response.json()
        else:
            content_value = unpackb(response.raw_bytes)

        if op.swagger_spec.config.get('validate_responses', False):
            validate_schema_object(op.swagger_spec, content_spec, content_value)

        return unmarshal_schema_object(
            swagger_spec=op.swagger_spec,
            schema_object_spec=content_spec,
            value=content_value,
        )

    if content_type.startswith('application'):
        return response.raw_bytes

    # TODO: Non-json response contents
    return response.text


def raise_on_unexpected(http_response):
    # type: (IncomingResponse) -> None
    """Raise an HTTPError if the response is 5XX.

    :param http_response: :class:`bravado_core.response.IncomingResponse`
    :raises: HTTPError
    """
    if 500 <= http_response.status_code <= 599:
        raise make_http_exception(response=http_response)


def raise_on_expected(http_response):
    # type: (IncomingResponse) -> None
    """Raise an HTTPError if the response is non-2XX and matches a response
    in the swagger spec.

    :param http_response: :class:`bravado_core.response.IncomingResponse`
    :raises: HTTPError
    """
    if not 200 <= http_response.status_code < 300:
        raise make_http_exception(
            response=http_response,
            swagger_result=http_response.swagger_result)
