from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
import udsoncan.tools as tools
from udsoncan.ResponseCode import ResponseCode
from udsoncan.exceptions import *
import struct

from typing import Optional, cast


class RoutineControl(BaseService):
    _sid = 0x31

    class ControlType(BaseSubfunction):
        """
        RoutineControl defined subfunctions
        """
        __pretty_name__ = 'control type'

        startRoutine = 1
        stopRoutine = 2
        requestRoutineResults = 3

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestSequenceError,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.SecurityAccessDenied,
                                   ResponseCode.GeneralProgrammingFailure
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: control_type_echo

                Requests subfunction echoed back by the server

        .. data:: routine_id_echo

                Requests routine ID echoed back by the server.

        .. data:: routine_status_record

                Additional data associated with the response.
        """

        control_type_echo: int
        routine_id_echo: int
        routine_status_record: bytes

        def __init__(self, control_type_echo: int, routine_id_echo: int, routine_status_record: bytes):
            super().__init__(RoutineControl)

            self.control_type_echo = control_type_echo
            self.routine_id_echo = routine_id_echo
            self.routine_status_record = routine_status_record

    class InterpretedResponse(Response):
        service_data: "RoutineControl.ResponseData"

    @classmethod
    def make_request(cls, routine_id: int, control_type: int, data: Optional[bytes] = None) -> Request:
        """
        Generates a request for RoutineControl

        :param routine_id: The routine ID. Value should be between 0 and 0xFFFF
        :type routine_id: int

        :param control_type: Service subfunction. Allowed values are from 0 to 0x7F
        :type control_type: int

        :param data: Optional additional data to provide to the server
        :type data: bytes

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        tools.validate_int(routine_id, min=0, max=0xFFFF, name='Routine ID')
        tools.validate_int(control_type, min=0, max=0x7F, name='Routine control type')

        if data is not None:
            if not isinstance(data, bytes):
                raise ValueError('data must be a valid bytes object')

        request = Request(service=cls, subfunction=control_type)
        request.data = struct.pack('>H', routine_id)
        if data is not None:
            request.data += data

        return request

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`RoutineControl.ResponseData<udsoncan.services.RoutineControl.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 3:
            raise InvalidResponseException(response, "Response data must be at least 3 bytes")

        response.service_data = cls.ResponseData(
            control_type_echo=response.data[0],
            routine_id_echo=struct.unpack(">H", response.data[1:3])[0],
            routine_status_record=response.data[3:] if len(response.data) > 3 else b''
        )

        return cast(RoutineControl.InterpretedResponse, response)
