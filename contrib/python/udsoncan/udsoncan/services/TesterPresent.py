from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
from udsoncan.exceptions import *
from udsoncan.Response import Response
from udsoncan.Request import Request

from typing import cast


class TesterPresent(BaseService):
    _sid = 0x3E

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: subfunction_echo

                Requests subfunction echoed back by the server. This value should always be 0
        """

        subfunction_echo: int

        def __init__(self, subfunction_echo: int):
            super().__init__(TesterPresent)
            self.subfunction_echo = subfunction_echo

    class InterpretedResponse(Response):
        service_data: "TesterPresent.ResponseData"

    @classmethod
    def make_request(cls) -> Request:
        """
        Generates a request for TesterPresent
        """
        return Request(service=cls, subfunction=0)

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`TesterPresent.ResponseData<udsoncan.services.TesterPresent.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, "Response data must be at least 1 bytes")

        response.service_data = cls.ResponseData(
            subfunction_echo=response.data[0]
        )

        return cast(TesterPresent.InterpretedResponse, response)
