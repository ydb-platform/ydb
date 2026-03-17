import struct

from udsoncan import DidCodec, check_did_config, make_did_codec_from_definition, fetch_codec_definition_from_config, DIDConfig
from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools

from typing import Dict, Any, Union, List, cast, Optional


class ReadDataByIdentifier(BaseService):
    _sid = 0x22
    _use_subfunction = False

    supported_negative_response = [ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.SecurityAccessDenied
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: values

                Dictionary mapping the DID (int) with the value returned by the associated :ref:`DidCodec<DidCodec>`.decode method
        """

        values: Dict[int, Any]

        def __init__(self, values: Dict[int, Any]):
            super().__init__(ReadDataByIdentifier)

            self.values = values

    class InterpretedResponse(Response):
        service_data: "ReadDataByIdentifier.ResponseData"

    @classmethod
    def validate_didlist_input(cls, dids: Union[int, List[int]]) -> List[int]:
        if not isinstance(dids, int) and not isinstance(dids, list):
            raise ValueError("Data Identifier must either be an integer or a list of integer")

        if isinstance(dids, int):
            tools.validate_int(dids, min=0, max=0xFFFF, name='Data Identifier')

        if isinstance(dids, list):
            for did in dids:
                tools.validate_int(did, min=0, max=0xFFFF, name='Data Identifier')

        return [dids] if not isinstance(dids, list) else dids

    @classmethod
    def make_request(cls, didlist: Union[int, List[int]], didconfig: Optional[DIDConfig]) -> Request:
        """
        Generates a request for ReadDataByIdentifier

        :param didlist: List of data identifier to read.
        :type didlist: list[int]

        :param didconfig: Optional definition of DID codecs for validation. Dictionary mapping a DID (int) to a valid :ref:`DidCodec<DidCodec>` class or pack/unpack string 
        :type didconfig: dict[int] = :ref:`DidCodec<DidCodec>`

        :raises ValueError: If parameters are out of range, missing or wrong type
        :raises ConfigError: If didlist contains a DID not defined in didconfig
        """

        didlist = cls.validate_didlist_input(didlist)

        req = Request(cls)
        if didconfig is not None:
            # Return a validated did config. Format may change, entries might be added if default value is set.
            check_did_config(didlist, didconfig)

            did_reading_all_data = None
            for did in didlist:
                # Make sure the config is good before sending the request. This method can raise.
                codec_definition = fetch_codec_definition_from_config(did, didconfig)
                codec = make_did_codec_from_definition(codec_definition)

                try:
                    len(codec)  # Validate the length function. May raise
                    if did_reading_all_data is not None:
                        raise ValueError('Did 0x%04X is configured to read the rest of the payload (__len__ raisong ReadAllRemainingData), but a subsequent DID is requested (0x%04x)' % (
                            did_reading_all_data, did))
                except DidCodec.ReadAllRemainingData:
                    if did_reading_all_data is not None:
                        raise ValueError('It is impossible to read 2 DIDs configured to read the rest of the payload (__len__ raising ReadAllRemainingData). Dids are : 0x%04X and 0x%04X' % (
                            did_reading_all_data, did))
                    did_reading_all_data = did

        req.data = struct.pack('>' + 'H' * len(didlist), *didlist)  # Encode list of DID

        return req

    @classmethod
    def interpret_response(cls,
                           response: Response,
                           didlist: Union[int, List[int]],
                           didconfig: DIDConfig,
                           tolerate_zero_padding: bool = True) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`ReadDataByIdentifier.ResponseData<udsoncan.services.ReadDataByIdentifier.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :param didlist:  List of data identifiers used for the request.
        :type didlist: list[int]

        :param didconfig: Definition of DID codecs. Dictionary mapping a DID (int) to a valid :ref:`DidCodec<DidCodec>` class or pack/unpack string 
        :type didconfig: dict[int] = :ref:`DidCodec<DidCodec>`

        :param tolerate_zero_padding: Ignore trailing zeros in the response data avoiding raising false :class:`InvalidResponseException<udsoncan.exceptions.InvalidResponseException>`.
        :type tolerate_zero_padding: bool

        :raises ValueError: If parameters are out of range, missing or wrong type
        :raises ConfigError: If ``didlist`` parameter or response contains a DID not defined in ``didconfig``.
        :raises InvalidResponseException: If response data is incomplete or if DID data does not match codec length.
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        didlist = cls.validate_didlist_input(didlist)
        didconfig_validated = check_did_config(didlist, didconfig)

        response.service_data = cls.ResponseData(
            values={}
        )

        # Parsing algorithm to extract DID value
        offset = 0
        while True:
            if len(response.data) <= offset:
                break  # Done

            if len(response.data) <= offset + 1:
                if tolerate_zero_padding and response.data[-1] == 0:  # One extra byte, but it's a 0 and we accept that. So we're done
                    break
                raise InvalidResponseException(response, "Response given by server is incomplete.")

            did = struct.unpack('>H', response.data[offset:offset + 2])[0]  # Get the DID number
            if did == 0 and did not in didconfig_validated and tolerate_zero_padding:  # We read two zeros and that is not a DID bu we accept that. So we're done.
                if response.data[offset:] == b'\x00' * (len(response.data) - offset):
                    break

            codec_definition = fetch_codec_definition_from_config(did, didconfig_validated)
            codec = make_did_codec_from_definition(codec_definition)
            offset += 2

            try:
                payload_size = len(codec)
            except DidCodec.ReadAllRemainingData:
                payload_size = len(response.data) - offset

            if len(response.data) < offset + payload_size:
                raise InvalidResponseException(
                    response, "Value for data identifier 0x%04x was incomplete according to definition in configuration" % did)

            subpayload = response.data[offset:offset + payload_size]
            offset += payload_size  # Codec must define a __len__ function that matches the encoded payload length.
            val = codec.decode(subpayload)
            response.service_data.values[did] = val

        return cast(ReadDataByIdentifier.InterpretedResponse, response)
