from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
from udsoncan.exceptions import *

from typing import Optional, cast


class RequestTransferExit(BaseService):
    _sid = 0x37
    _use_subfunction = False
    _no_response_data = True

    supported_negative_response = [ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.RequestSequenceError,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.GeneralProgrammingFailure
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: parameter_records

                bytes object containing optional data provided by the server
        """

        parameter_records: bytes

        def __init__(self, parameter_records: bytes):
            super().__init__(RequestTransferExit)
            self.parameter_records = parameter_records

    class InterpretedResponse(Response):
        service_data: "RequestTransferExit.ResponseData"

    @classmethod
    def make_request(cls, data: Optional[bytes] = None) -> Request:
        """
        Generates a request for RequestTransferExit

        :param data: Additional optional data to send to the server
        :type data: bytes

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        if data is not None and not isinstance(data, bytes):
            raise ValueError('data must be a bytes object')

        request = Request(service=cls, data=data)
        return request

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`RequestTransferExit.ResponseData<udsoncan.services.RequestTransferExit.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`
        """
        response.service_data = cls.ResponseData(
            parameter_records=response.data if response.data else bytes()
        )

        return cast(RequestTransferExit.InterpretedResponse, response)
