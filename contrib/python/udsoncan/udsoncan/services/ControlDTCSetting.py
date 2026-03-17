from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Optional, cast


class ControlDTCSetting(BaseService):
    _sid = 0x85

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestOutOfRange
                                   ]

    class SettingType(BaseSubfunction):
        """
        ControlDTCSetting defined subfunctions
        """

        __pretty_name__ = 'setting type'

        on = 1
        off = 2
        vehicleManufacturerSpecific = (0x40, 0x5F)  # To be able to print textual name for logging only.
        systemSupplierSpecific = (0x60, 0x7E)		# To be able to print textual name for logging only.

    class ResponseData(BaseResponseData):
        """
        .. data:: setting_type_echo

                Request subfunction echoed back by the server
        """

        def __init__(self, setting_type_echo: int):
            super().__init__(ControlDTCSetting)
            self.setting_type_echo = setting_type_echo

    class InterpretedResponse(Response):
        service_data: "ControlDTCSetting.ResponseData"

    @classmethod
    def make_request(cls, setting_type: int, data: Optional[bytes] = None) -> Request:
        """
        Generates a request for ControlDTCSetting

        :param setting_type: Service subfunction. Allowed values are from 0 to 0x7F
        :type setting_type: int

        :param data: Optional additional data sent with the request called `DTCSettingControlOptionRecord`
        :type data: bytes

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        tools.validate_int(setting_type, min=0, max=0x7F, name='Setting type')
        if data is not None:
            if not isinstance(data, bytes):
                raise ValueError('data must be a valid bytes object')

        return Request(service=cls, subfunction=setting_type, data=data)

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`ControlDTCSetting.ResponseData<udsoncan.services.ControlDTCSetting.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, "Response data must be at least 1 byte")

        response.service_data = cls.ResponseData(
            setting_type_echo=response.data[0]
        )

        return cast(ControlDTCSetting.InterpretedResponse, response)
