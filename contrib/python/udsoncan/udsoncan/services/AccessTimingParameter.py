from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Optional, cast


class AccessTimingParameter(BaseService):
    _sid = 0x83

    class AccessType(BaseSubfunction):
        """
        AccessTimingParameter defined subfunctions
        """

        __pretty_name__ = 'access type'

        readExtendedTimingParameterSet = 1
        setTimingParametersToDefaultValues = 2
        readCurrentlyActiveTimingParameters = 3
        setTimingParametersToGivenValues = 4

    class ResponseData(BaseResponseData):
        """
        .. data:: access_type_echo

                Request subfunction echoed back by the server

        .. data:: timing_param_record

                Additional data associated with the response.
        """

        access_type_echo: int
        timing_param_record: bytes

        def __init__(self, access_type_echo: int, timing_param_record: bytes):
            super().__init__(AccessTimingParameter)
            self.access_type_echo = access_type_echo
            self.timing_param_record = timing_param_record

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestOutOfRange
                                   ]

    class InterpretedResponse(Response):
        service_data: "AccessTimingParameter.ResponseData"

    @classmethod
    def make_request(cls, access_type: int, timing_param_record: Optional[bytes] = None) -> Request:
        """
        Generates a request for AccessTimingParameter

        :param access_type: Service subfunction. Allowed values are from 0 to 0x7F
        :type access_type: int

        :param timing_param_record: Data associated with request. Must be present only when access_type=``AccessType.setTimingParametersToGivenValues`` (4)
        :type timing_param_record: bytes

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        tools.validate_int(access_type, min=0, max=0x7F, name='Access type')

        if timing_param_record is not None and access_type != cls.AccessType.setTimingParametersToGivenValues:
            raise ValueError('timing_param_record can only be set when access_type is setTimingParametersToGivenValues"')

        if timing_param_record is None and access_type == cls.AccessType.setTimingParametersToGivenValues:
            raise ValueError('A timing_param_record must be provided when access_type is "setTimingParametersToGivenValues"')

        request = Request(service=cls, subfunction=access_type, data=bytes())
        assert request.data is not None

        if timing_param_record is not None:
            if not isinstance(timing_param_record, bytes):
                raise ValueError("timing_param_record must be a valid bytes objects")
            request.data += timing_param_record

        return request

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`AccessTimingParameter.ResponseData<udsoncan.services.AccessTimingParameter.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, "Response data must be at least 1 byte")

        response.service_data = cls.ResponseData(
            access_type_echo=response.data[0],
            timing_param_record=response.data[1:] if len(response.data) > 1 else b''
        )
        return cast(AccessTimingParameter.InterpretedResponse, response)
