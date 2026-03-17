from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Optional, cast


class ECUReset(BaseService):
    _sid = 0x11

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.SecurityAccessDenied
                                   ]

    class ResetType(BaseSubfunction):
        """
        ECUReset defined subfunctions
        """
        __pretty_name__ = 'reset type'  # Only to print "custom reset type" instead of "custom subfunction"

        hardReset = 1
        keyOffOnReset = 2
        softReset = 3
        enableRapidPowerShutDown = 4
        disableRapidPowerShutDown = 5

    class ResponseData(BaseResponseData):
        """
        .. data:: reset_type_echo

                Request subfunction echoed back by the server

        .. data:: powerdown_time

                Amount of time, in seconds, before the power down sequence is executed. Should be provided only when reset type is enableRapidPowerShutDown
        """

        reset_type_echo: int
        powerdown_time: Optional[int]

        def __init__(self, reset_type_echo: int, powerdown_time: Optional[int] = None):
            super().__init__(ECUReset)

            self.reset_type_echo = reset_type_echo
            self.powerdown_time = powerdown_time

    class InterpretedResponse(Response):
        service_data: "ECUReset.ResponseData"

    @classmethod
    def make_request(cls, reset_type: int) -> Request:
        """
        Generates a request for ECUReset

        :param reset_type: Service subfunction. Allowed values are from 0 to 0x7F
        :type reset_type: int

        :raises ValueError: If parameters are out of range, missing or wrong type
        """
        tools.validate_int(reset_type, min=0, max=0x7F, name='Reset type')
        return Request(service=cls, subfunction=reset_type)

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`ECUReset.ResponseData<udsoncan.services.ECUReset.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1: 	# Should not happen as response decoder will raise an exception.
            raise InvalidResponseException(response, "Response data must be at least 1 bytes")

        response.service_data = cls.ResponseData(
            reset_type_echo=response.data[0]
        )

        if response.service_data.reset_type_echo == cls.ResetType.enableRapidPowerShutDown:
            if len(response.data) < 2:
                raise InvalidResponseException(response, 'Response data is missing a second byte for reset type "enableRapidPowerShutDown"')

            response.service_data.powerdown_time = response.data[1]

        return cast(ECUReset.InterpretedResponse, response)
