from udsoncan import MemoryLocation
from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode

from typing import cast


class ReadMemoryByAddress(BaseService):
    _sid = 0x23
    _use_subfunction = False

    supported_negative_response = [ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.SecurityAccessDenied
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: memory_block

                bytes object reflecting the content of the read memory
        """

        memory_block: bytes

        def __init__(self, memory_block: bytes):
            super().__init__(ReadMemoryByAddress)
            self.memory_block = memory_block

    class InterpretedResponse(Response):
        service_data: "ReadMemoryByAddress.ResponseData"

    @classmethod
    def make_request(cls, memory_location: MemoryLocation) -> Request:
        """
        Generates a request for ReadMemoryByAddress

        :param memory_location: The address and the size of the memory block to read.
        :type memory_location: :ref:`MemoryLocation <MemoryLocation>`

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        if not isinstance(memory_location, MemoryLocation):
            raise ValueError('Given memory location must be an instance of MemoryLocation')

        request = Request(service=cls)
        request.data = bytes()
        request.data += memory_location.alfid.get_byte()  # AddressAndLengthFormatIdentifier
        request.data += memory_location.get_address_bytes()
        request.data += memory_location.get_memorysize_bytes()

        return request

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`ReadMemoryByAddress.ResponseData<udsoncan.services.ReadMemoryByAddress.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, "Response data must be at least 1 byte")

        response.service_data = cls.ResponseData(
            memory_block=response.data
        )

        return cast(ReadMemoryByAddress.InterpretedResponse, response)
