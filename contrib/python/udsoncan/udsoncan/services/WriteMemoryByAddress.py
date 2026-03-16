from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan import MemoryLocation
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
from udsoncan.exceptions import *

from typing import cast


class WriteMemoryByAddress(BaseService):
    _sid = 0x3D
    _use_subfunction = False

    supported_negative_response = [ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.SecurityAccessDenied,
                                   ResponseCode.GeneralProgrammingFailure
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: alfid_echo

                AddressAndLengthFormatIdentifier used in the :ref:`MemoryLocation <MemoryLocation>` object echoed back by the server as a single byte integer.

        .. data:: memory_location_echo

                An instance of :ref:`MemoryLocation <MemoryLocation>` that includes the address, size and alfid that the server echoed back.		
        """

        alfid_echo: int
        memory_location_echo: MemoryLocation

        def __init__(self, alfid_echo: int, memory_location_echo: MemoryLocation):
            super().__init__(WriteMemoryByAddress)
            self.alfid_echo = alfid_echo
            self.memory_location_echo = memory_location_echo

    class InterpretedResponse(Response):
        service_data: "WriteMemoryByAddress.ResponseData"

    @classmethod
    def make_request(cls, memory_location: MemoryLocation, data: bytes) -> Request:
        """
        Generates a request for ReadMemoryByAddress

        :param memory_location: The address and the size of the memory block to write.
        :type memory_location: :ref:`MemoryLocation <MemoryLocation>`

        :param data: The data to write into memory.
        :type data: bytes

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        if not isinstance(memory_location, MemoryLocation):
            raise ValueError('Given memory location must be an instance of MemoryLocation')

        if not isinstance(data, bytes):
            raise ValueError('data must be a bytes object')
        request = Request(service=cls)

        request.data = bytes()
        request.data += memory_location.alfid.get_byte()  # AddressAndLengthFormatIdentifier
        request.data += memory_location.get_address_bytes()
        request.data += memory_location.get_memorysize_bytes()
        request.data += data

        return request

    @classmethod
    def interpret_response(cls, response: Response, memory_location: MemoryLocation) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`WriteMemoryByAddress.ResponseData<udsoncan.services.WriteMemoryByAddress.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :param memory_location: The memory location used for the request. 
                The bytes position varies depending on the ``memory_location`` format
        :type memory_location: :ref:`MemoryLocation <MemoryLocation>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """

        if not isinstance(memory_location, MemoryLocation):
            raise ValueError('Given memory location must be an instance of MemoryLocation')

        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        address_bytes = memory_location.get_address_bytes()
        memorysize_bytes = memory_location.get_memorysize_bytes()

        expected_response_size = 1 + len(address_bytes) + len(memorysize_bytes)
        if len(response.data) < expected_response_size:
            raise InvalidResponseException(response, 'Repsonse should be at least %d bytes' % (expected_response_size))

        offset = 1
        length = len(memory_location.get_address_bytes())
        address_echo = response.data[1:1 + length]
        offset += length
        length = len(memory_location.get_memorysize_bytes())
        memorysize_echo = response.data[offset:offset + length]

        response.service_data = cls.ResponseData(
            alfid_echo=response.data[0],
            memory_location_echo=MemoryLocation.from_bytes(address_bytes=address_echo, memorysize_bytes=memorysize_echo)
        )

        return cast(WriteMemoryByAddress.InterpretedResponse, response)
