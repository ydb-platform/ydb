# -*- coding: utf-8 -*-
import typing

import monotonic
from bravado_core.response import IncomingResponse

if getattr(typing, 'TYPE_CHECKING', False):  # Needed to avoid cyclic import.
    from bravado.config import RequestConfig


T = typing.TypeVar('T')


class BravadoResponse(typing.Generic[T]):
    """Bravado response object containing the swagger result as well as response metadata.

    :ivar result: Swagger result from the server
    :ivar BravadoResponseMetadata metadata: metadata for this response including HTTP response
    """

    def __init__(
        self,
        result,  # type: typing.Optional[T]
        metadata,  # type: 'BravadoResponseMetadata[T]'
    ):
        # type: (...) -> None
        self.result = result
        self.metadata = metadata

    @property
    def incoming_response(self):
        # type: () -> IncomingResponse
        return self.metadata.incoming_response


class BravadoResponseMetadata(typing.Generic[T]):
    """HTTP response metadata.

    NOTE: The `elapsed_time` attribute might be slightly lower than the actual time spent since calling
    the operation object, as we only start measuring once the call to `HTTPClient.request` returns.
    Nevertheless, it should be accurate enough for logging and debugging, i.e. determining what went
    on and how much time was spent waiting for the response.

    :ivar float start_time: monotonic timestamp at which the future was created
    :ivar float request_end_time: monotonic timestamp at which we received the HTTP response
    :ivar float processing_end_time: monotonic timestamp at which processing the response ended
    :ivar tuple handled_exception_info: 3-tuple of exception class, exception instance and string
        representation of the traceback in case an exception was caught during request processing.
    """

    def __init__(
        self,
        incoming_response,  # type: typing.Optional[IncomingResponse]
        swagger_result,  # type: typing.Optional[T]
        start_time,  # type: float
        request_end_time,  # type: float
        handled_exception_info,  # type: typing.Optional[typing.List[typing.Union[typing.Type[BaseException], BaseException, typing.Text]]]  # noqa
        request_config,  # type: RequestConfig
    ):
        # type: (...) -> None
        """
        :param incoming_response: a subclass of bravado_core.response.IncomingResponse.
        :param swagger_result: the unmarshalled result that is being returned to the user.
        :param start_time: monotonic timestamp indicating when the HTTP future was created. Depending on the
            internal operation of the HTTP client used, this is either before the HTTP request was initiated
            (default client) or right after the HTTP request was sent (e.g. bravado-asyncio / fido).
        :param request_end_time: monotonic timestamp indicating when we received the incoming response,
            excluding unmarshalling, validation or potential fallback result processing.
        :param handled_exception_info: sys.exc_info() data if an exception was caught and handled as
            part of a fallback response; note that the third element in the list is a string representation
            of the traceback, not a traceback object.
        :param RequestConfig request_config: namedtuple containing the request options that were used
            for making this request.
        """
        self._incoming_response = incoming_response
        self.start_time = start_time
        self.request_end_time = request_end_time
        self.processing_end_time = monotonic.monotonic()
        self.handled_exception_info = handled_exception_info
        self.request_config = request_config

        # we expose the result to the user through the BravadoResponse object;
        # we're passing it in to this object in case custom implementations need it
        self._swagger_result = swagger_result

    @property
    def incoming_response(self):
        # type: () -> IncomingResponse
        if not self._incoming_response:
            raise ValueError('No incoming_response present')
        return self._incoming_response

    @property
    def status_code(self):
        # type: () -> int
        return self.incoming_response.status_code

    @property
    def headers(self):
        # type: () -> typing.Mapping[typing.Text, typing.Text]
        return self.incoming_response.headers

    @property
    def is_fallback_result(self):
        # type: () -> bool
        return bool(self.handled_exception_info)

    @property
    def request_elapsed_time(self):
        # type: () -> float
        return self.request_end_time - self.start_time

    @property
    def elapsed_time(self):
        # type: () -> float
        return self.processing_end_time - self.start_time
