import struct
from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan import latest_standard
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Optional, cast


class DiagnosticSessionControl(BaseService):
    _sid = 0x10

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect
                                   ]

    class Session(BaseSubfunction):
        """
        DiagnosticSessionControl defined subfunctions
        """
        __pretty_name__ = 'session'

        defaultSession = 1
        programmingSession = 2
        extendedDiagnosticSession = 3
        safetySystemDiagnosticSession = 4

    class ResponseData(BaseResponseData):
        """
        .. data:: session_echo

                Request subfunction echoed back by the server

        .. data:: session_param_records

                Raw session parameter records. Data given by the server. For 2006 configurations, this data can is manufacturer specific. For 2013 version and above, this data correspond to P2 and P2* timing requirement.

        .. data:: p2_server_max

                Default P2 max timing supported by the server for the activated diagnostic session. Applicable for 2013 version and above. Value in seconds.

        .. data:: p2_star_server_max

                Default P2* (NRC 0x78) max timing supported by the server for the activated diagnostic session. Applicable for 2013 version and above. Value in seconds
        """
        session_echo: int
        session_param_records: bytes
        p2_server_max: Optional[float]
        p2_star_server_max: Optional[float]

        def __init__(self, session_echo: int, session_param_records: bytes, p2_server_max: Optional[float] = None, p2_star_server_max: Optional[float] = None):
            super().__init__(DiagnosticSessionControl)
            self.session_echo = session_echo
            self.session_param_records = session_param_records
            self.p2_server_max = p2_server_max
            self.p2_star_server_max = p2_star_server_max

    class InterpretedResponse(Response):
        service_data: "DiagnosticSessionControl.ResponseData"

    @classmethod
    def make_request(cls, session: int) -> Request:
        """
        Generates a request for DiagnosticSessionControl service

        :param session: Service subfunction. Allowed values are from 0 to 0x7F
        :type session: int

        :raises ValueError: If parameters are out of range, missing or wrong type
        """

        tools.validate_int(session, min=0, max=0x7F, name='Session number')
        return Request(service=cls, subfunction=session)

    @classmethod
    def interpret_response(cls, response: Response, standard_version: int = latest_standard) -> "DiagnosticSessionControl.InterpretedResponse":
        """
        Populates the response ``service_data`` property with an instance of :class:`DiagnosticSessionControl.ResponseData<udsoncan.services.DiagnosticSessionControl.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :param standard_version: The version of the ISO-14229 (the year). eg. 2006, 2013, 2020
        :type standard_version: int

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1: 	# Should not happen as response decoder will raise an exception.
            raise InvalidResponseException(response, "Response data must be at least 1 bytes")

        response.service_data = cls.ResponseData(
            session_echo=response.data[0],
            session_param_records=response.data[1:] if len(response.data) > 1 else b''
        )

        if (standard_version >= 2013):
            if len(response.data) != 5:
                raise InvalidResponseException(
                    response, 'Response must contain 4 bytes of data representing the server timing requirements (P2 and P2* timeouts). Got %d bytes' % len(response.data))

            (a, b) = struct.unpack('>HH', response.data[1:])
            response.service_data.p2_server_max = (a) / 1000
            response.service_data.p2_star_server_max = (b * 10) / 1000

        return cast(DiagnosticSessionControl.InterpretedResponse, response)
