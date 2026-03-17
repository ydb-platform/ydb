# -*- coding: utf-8 -*-
import typing

from bravado_core.response import IncomingResponse
from typing_extensions import overload  # typing.overload won't work on Python 3.5.0/3.5.1

from bravado.config import RequestConfig
from bravado.exception import BravadoTimeoutError
from bravado.http_future import _SENTINEL
from bravado.http_future import FALLBACK_EXCEPTIONS
from bravado.http_future import SENTINEL
from bravado.response import BravadoResponse
from bravado.response import BravadoResponseMetadata


T = typing.TypeVar('T')


class IncomingResponseMock(IncomingResponse):
    def __init__(self, status_code, **kwargs):
        self.headers = {}  # type: typing.Mapping[str, str]
        self.status_code = status_code
        for name, value in kwargs.items():
            setattr(self, name, value)


class BravadoResponseMock(typing.Generic[T]):
    """Class that behaves like the :meth:`.HttpFuture.response` method as well as a :class:`.BravadoResponse`.
    Please check the documentation for further information.
    """

    def __init__(self, result, metadata=None):
        # type: (T, typing.Optional[BravadoResponseMetadata[T]]) -> None
        self._result = result
        if metadata:
            self._metadata = metadata
        else:
            self._metadata = BravadoResponseMetadata(
                incoming_response=IncomingResponseMock(status_code=200),
                swagger_result=self._result,
                start_time=1528733800,
                request_end_time=1528733801,
                handled_exception_info=None,
                request_config=RequestConfig({}, also_return_response_default=False),
            )

    def __call__(
        self,
        timeout=None,  # type: typing.Optional[float]
        fallback_result=SENTINEL,  # type: typing.Union[_SENTINEL, T, typing.Callable[[BaseException], T]]  # noqa
        exceptions_to_catch=FALLBACK_EXCEPTIONS,  # type: typing.Tuple[typing.Type[BaseException], ...]
    ):
        # type: (...) -> BravadoResponse[T]
        return self  # type: ignore

    @property
    def result(self):
        return self._result

    @property
    def metadata(self):
        return self._metadata


class FallbackResultBravadoResponseMock(object):
    """Class that behaves like the :meth:`.HttpFuture.response` method as well as a :class:`.BravadoResponse`.
    It will always call the ``fallback_result`` callback that's passed to the ``response()`` method.
    Please check the documentation for further information.
    """

    def __init__(self, exception=BravadoTimeoutError(), metadata=None):
        # type: (BaseException, typing.Optional[BravadoResponseMetadata]) -> None
        self._exception = exception
        if metadata:
            self._metadata = metadata
        else:
            self._metadata = BravadoResponseMetadata(
                incoming_response=IncomingResponse(),
                swagger_result=None,  # we're going to set it later
                start_time=1528733800,
                request_end_time=1528733801,
                handled_exception_info=[self._exception.__class__, self._exception, 'Traceback'],
                request_config=RequestConfig({}, also_return_response_default=False),
            )

    @overload
    def __call__(
        self,
        timeout=None,  # type: typing.Optional[float]
        fallback_result=T,  # type: T
        exceptions_to_catch=FALLBACK_EXCEPTIONS,  # type: typing.Tuple[typing.Type[BaseException], ...]
    ):
        # type: (...) -> BravadoResponse[T]
        pass

    @overload  # noqa: F811
    def __call__(
        self,
        timeout=None,  # type: typing.Optional[float]
        fallback_result=lambda x: None,  # typing.Callable[[BaseException], T]
        exceptions_to_catch=FALLBACK_EXCEPTIONS,  # type: typing.Tuple[typing.Type[BaseException], ...]
    ):
        # type: (...) -> BravadoResponse[T]
        pass

    def __call__(  # noqa: F811
        self,
        timeout=None,  # type: typing.Optional[float]
        fallback_result=SENTINEL,  # type: typing.Union[_SENTINEL, T, typing.Callable[[BaseException], T]]  # noqa
        exceptions_to_catch=FALLBACK_EXCEPTIONS,  # type: typing.Tuple[typing.Type[BaseException], ...]
    ):
        # type: (...) -> BravadoResponse[T]
        assert not isinstance(fallback_result, _SENTINEL), 'You\'re using FallbackResultBravadoResponseMock without' \
            ' a fallback_result. Either provide one or use BravadoResponseMock.'
        self._fallback_result = fallback_result(self._exception) if callable(fallback_result) else fallback_result
        self._metadata._swagger_result = self._fallback_result
        return self  # type: ignore

    @property
    def result(self):
        return self._fallback_result

    @property
    def metadata(self):
        return self._metadata
