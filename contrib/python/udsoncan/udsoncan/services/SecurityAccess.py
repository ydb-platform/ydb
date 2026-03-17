from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Optional, cast


class SecurityAccess(BaseService):
    _sid = 0x27

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestSequenceError,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.InvalidKey,
                                   ResponseCode.ExceedNumberOfAttempts,
                                   ResponseCode.RequiredTimeDelayNotExpired
                                   ]

    class Mode:
        RequestSeed = 0
        SendKey = 1

    class ResponseData(BaseResponseData):
        """
        .. data:: security_level_echo

                Requests subfunction echoed back by the server

        .. data:: seed

                Seed value. Only present if request mode was ``RequestSeed`` (even subfunction)
        """

        security_level_echo: int
        seed: Optional[bytes]

        def __init__(self, security_level_echo: int, seed: Optional[bytes] = None):
            super().__init__(SecurityAccess)

            self.security_level_echo = security_level_echo
            self.seed = seed

    class InterpretedResponse(Response):
        service_data: "SecurityAccess.ResponseData"

    @classmethod
    def normalize_level(cls, mode: int, level: int) -> int:
        cls.validate_mode(mode)
        tools.validate_int(level, min=1, max=0x7E, name='Security level')

        if mode == cls.Mode.RequestSeed:
            return level if level % 2 == 1 else level - 1
        elif mode == cls.Mode.SendKey:
            return level if level % 2 == 0 else level + 1

        raise ValueError("Unsupported mode")

    @classmethod
    def make_request(cls, level: int, mode: int, data=bytes()) -> Request:
        """
        Generates a request for SecurityAccess

        :param level: Service subfunction. The security level to unlock. Value ranging from 0 to 7F 
                For mode=``RequestSeed`` (0), level must be an odd value. For mode=``SendKey`` (1), level must be an even value.
                If the even/odd constraint is not respected, the level value will be corrected to properly set the LSB.
        :type level: int

        :param mode: Type of request to perform. ``SecurityAccess.Mode.RequestSeed`` or ``SecurityAccess.Mode.SendKey`` 
        :type mode: SecurityAccess.Mode, int

        :param data: securityAccessDataRecord (optional) for the get seed, securityKey (required) for the send key
        :type data: bytes

        :raises ValueError: If parameters are out of range, missing or wrong type
        """
        cls.validate_mode(mode)

        tools.validate_int(level, min=0, max=0x7F, name='Security level')
        req = Request(service=cls, subfunction=cls.normalize_level(mode=mode, level=level))

        if not isinstance(data, bytes):
            raise ValueError('key must be a valid bytes object')
        req.data = data

        return req

    @classmethod
    def interpret_response(cls, response: Response, mode: int) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`SecurityAccess.ResponseData<udsoncan.services.SecurityAccess.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        :raises ValueError: If mode is not ``RequestSeed`` or ``SendKey``
        """

        cls.validate_mode(mode)

        minlength = 2 if mode == cls.Mode.RequestSeed else 1

        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < minlength:
            raise InvalidResponseException(response, "Response data must be at least %d bytes" % (minlength))

        response.service_data = cls.ResponseData(
            security_level_echo=response.data[0]
        )

        if mode == cls.Mode.RequestSeed:
            response.service_data.seed = response.data[1:]

        return cast(SecurityAccess.InterpretedResponse, response)

    @classmethod
    def validate_mode(cls, mode: int):
        if mode not in (cls.Mode.RequestSeed, cls.Mode.SendKey):
            raise ValueError('Given mode must be either be RequestSeed (%d) or SendKey (%d).' % (cls.Mode.RequestSeed, cls.Mode.SendKey))
