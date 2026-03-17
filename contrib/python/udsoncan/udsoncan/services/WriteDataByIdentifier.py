import struct
from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan import DidCodec, check_did_config, make_did_codec_from_definition, fetch_codec_definition_from_config, DIDConfig
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Any, cast


class WriteDataByIdentifier(BaseService):
    _sid = 0x2E
    _use_subfunction = False

    supported_negative_response = [ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.SecurityAccessDenied,
                                   ResponseCode.GeneralProgrammingFailure
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: did_echo

                The DID echoed back by the server
        """

        did_echo: int

        def __init__(self, did_echo: int):
            super().__init__(WriteDataByIdentifier)

            self.did_echo = did_echo

    class InterpretedResponse(Response):
        service_data: "WriteDataByIdentifier.ResponseData"

    @classmethod
    def make_request(cls, did: int, value: Any, didconfig: DIDConfig) -> Request:
        """
        Generates a request for WriteDataByIdentifier

        :param did: The data identifier to write
        :type did: int

        :param value: Value given to the :ref:`DidCodec <DidCodec>`.encode method. If involved codec is defined with a pack string (default codec), multiple values may be passed with a tuple.
        :type value: object

        :param didconfig: Definition of DID codecs. Dictionary mapping a DID (int) to a valid :ref:`DidCodec <DidCodec>` class or pack/unpack string 
        :type didconfig: dict[int] = :ref:`DidCodec <DidCodec>`

        :raises ValueError: If parameters are out of range, missing or wrong type
        :raises ConfigError: If ``didlist`` contains a DID not defined in ``didconfig``
        """

        tools.validate_int(did, min=0, max=0xFFFF, name='Data Identifier')
        req = Request(cls)
        didconfig = check_did_config(did, didconfig=didconfig)  # Make sure all DIDs are correctly defined in client config
        codec_definition = fetch_codec_definition_from_config(did, didconfig)
        req.data = struct.pack('>H', did)  # encode DID number
        codec = make_did_codec_from_definition(codec_definition)
        if codec.__class__ == DidCodec and isinstance(value, tuple):
            req.data += codec.encode(*value)    # Fixes issue #29
        else:
            req.data += codec.encode(value)

        return req

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`WriteDataByIdentifier.ResponseData<udsoncan.services.WriteDataByIdentifier.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 2:
            raise InvalidResponseException(response, "Response must be at least 2 bytes long")

        response.service_data = cls.ResponseData(
            did_echo=struct.unpack(">H", response.data[0:2])[0]
        )

        return cast(WriteDataByIdentifier.InterpretedResponse, response)
