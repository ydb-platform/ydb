import struct
from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
from udsoncan.exceptions import *
import udsoncan.tools as tools

from typing import Optional, cast


class TransferData(BaseService):
    _sid = 0x36
    _use_subfunction = False

    supported_negative_response = [ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.RequestSequenceError,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.TransferDataSuspended,
                                   ResponseCode.GeneralProgrammingFailure,
                                   ResponseCode.WrongBlockSequenceCounter,
                                   ResponseCode.VoltageTooHigh,
                                   ResponseCode.VoltageTooLow
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: sequence_number_echo

                Requests subfunction echoed back by the server

        .. data:: parameter_records

                Optional additional data associated with the response.
        """

        sequence_number_echo: int
        parameter_records: bytes

        def __init__(self, sequence_number_echo: int, parameter_records: bytes):
            super().__init__(TransferData)
            self.sequence_number_echo = sequence_number_echo
            self.parameter_records = parameter_records

    class InterpretedResponse(Response):
        service_data: "TransferData.ResponseData"

    @classmethod
    def make_request(cls, sequence_number: int, data: Optional[bytes] = None) -> Request:
        """
        Generates a request for TransferData

        :param sequence_number: Corresponds to an 8bit counter that should increment for each new block transferred.
                Allowed values are from 0 to 0xFF
        :type sequence_number: int

        :param data: Optional additional data to send to the server
        :type data: bytes

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        tools.validate_int(sequence_number, min=0, max=0xFF, name='Block sequence counter')  # Not a subfunction!

        if data is not None and not isinstance(data, bytes):
            raise ValueError('data must be a bytes object')

        request = Request(service=cls)
        request.data = struct.pack('B', sequence_number)

        if data is not None:
            request.data += data
        return request

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`TransferData.ResponseData<udsoncan.services.TransferData.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, "Response data must be at least 1 bytes")

        response.service_data = cls.ResponseData(
            sequence_number_echo=response.data[0],
            parameter_records=response.data[1:] if len(response.data) > 1 else bytes()
        )

        return cast(TransferData.InterpretedResponse, response)
