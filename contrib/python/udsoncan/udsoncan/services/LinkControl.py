from udsoncan import Baudrate
from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Optional, cast


class LinkControl(BaseService):
    _sid = 0x87

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestSequenceError,
                                   ResponseCode.RequestOutOfRange
                                   ]

    class ControlType(BaseSubfunction):
        """
        LinkControl defined subfunctions
        """
        __pretty_name__ = 'control type'

        verifyBaudrateTransitionWithFixedBaudrate = 1
        verifyBaudrateTransitionWithSpecificBaudrate = 2
        transitionBaudrate = 3

    class ResponseData(BaseResponseData):
        """
        .. data:: control_type_echo

                Request subfunction echoed back by the server
        """

        control_type_echo: int

        def __init__(self, control_type_echo: int):
            super().__init__(LinkControl)
            self.control_type_echo = control_type_echo

    class InterpretedResponse(Response):
        service_data: "LinkControl.ResponseData"

    @classmethod
    def make_request(cls, control_type: int, baudrate: Optional[Baudrate] = None) -> Request:
        """
        Generates a request for LinkControl

        :param control_type: Service subfunction. Allowed values are from 0 to 0x7F
        :type control_type: int

        :param baudrate: Required baudrate value when ``control_type`` is either ``verifyBaudrateTransitionWithFixedBaudrate`` (1) or ``verifyBaudrateTransitionWithSpecificBaudrate`` (2)
        :type baudrate: :ref:`Baudrate <Baudrate>`

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        tools.validate_int(control_type, min=0, max=0x7F, name='Control type')

        if control_type in [cls.ControlType.verifyBaudrateTransitionWithSpecificBaudrate, cls.ControlType.verifyBaudrateTransitionWithFixedBaudrate]:
            if baudrate is None:
                raise ValueError('A Baudrate must be provided with control type : "verifyBaudrateTransitionWithSpecificBaudrate" (0x%02x) or "verifyBaudrateTransitionWithFixedBaudrate" (0x%02x)' % (
                    cls.ControlType.verifyBaudrateTransitionWithSpecificBaudrate, cls.ControlType.verifyBaudrateTransitionWithFixedBaudrate))

            if not isinstance(baudrate, Baudrate):
                raise ValueError('Given baudrate must be an instance of the Baudrate class')
        else:
            if baudrate is not None:
                raise ValueError('The baudrate parameter is only needed when control type is "verifyBaudrateTransitionWithSpecificBaudrate" (0x%02x) or "verifyBaudrateTransitionWithFixedBaudrate" (0x%02x)' % (
                    cls.ControlType.verifyBaudrateTransitionWithSpecificBaudrate, cls.ControlType.verifyBaudrateTransitionWithFixedBaudrate))

        if control_type == cls.ControlType.verifyBaudrateTransitionWithSpecificBaudrate:
            assert baudrate is not None
            baudrate = baudrate.make_new_type(Baudrate.Type.Specific)

        if control_type == cls.ControlType.verifyBaudrateTransitionWithFixedBaudrate:
            assert baudrate is not None
            if baudrate.baudtype == Baudrate.Type.Specific:
                baudrate = baudrate.make_new_type(Baudrate.Type.Fixed)

        request = Request(service=cls, subfunction=control_type)
        if baudrate is not None:
            request.data = baudrate.get_bytes()
        return request

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`LinkControl.ResponseData<udsoncan.services.LinkControl.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, "Response data must be at least 1 bytes")

        response.service_data = cls.ResponseData(
            control_type_echo=response.data[0]
        )

        return cast(LinkControl.InterpretedResponse, response)
