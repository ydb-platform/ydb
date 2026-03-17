from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan import CommunicationType
from udsoncan.exceptions import *
from udsoncan.BaseService import BaseService, BaseSubfunction, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import udsoncan.tools as tools
from udsoncan import latest_standard
import struct

from typing import cast, Union, Optional


class CommunicationControl(BaseService):
    _sid = 0x28

    class ControlType(BaseSubfunction):
        """
        CommunicationControl defined subfunctions
        """
        __pretty_name__ = 'control type'

        enableRxAndTx = 0
        enableRxAndDisableTx = 1
        disableRxAndEnableTx = 2
        disableRxAndTx = 3
        enableRxAndDisableTxWithEnhancedAddressInformation = 4
        enableRxAndTxWithEnhancedAddressInformation = 5

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestOutOfRange
                                   ]

    class ResponseData(BaseResponseData):
        """
        .. data:: control_type_echo

                Request subfunction echoed back by the server
        """
        control_type_echo: int

        def __init__(self, control_type_echo: int):
            super().__init__(CommunicationControl)
            self.control_type_echo = control_type_echo

    class InterpretedResponse(Response):
        service_data: "CommunicationControl.ResponseData"

    @classmethod
    def normalize_communication_type(self, communication_type: Union[int, bytes, CommunicationType]) -> CommunicationType:
        if not isinstance(communication_type, CommunicationType) and not isinstance(communication_type, int) and not isinstance(communication_type, bytes):
            raise ValueError('communication_type must either be a CommunicationType object or an integer')

        if isinstance(communication_type, int) or isinstance(communication_type, bytes):
            communication_type = CommunicationType.from_byte(communication_type)

        return communication_type

    @classmethod
    def make_request(cls, control_type: int, communication_type: CommunicationType, node_id: Optional[int] = None, standard_version=latest_standard) -> Request:
        """
        Generates a request for CommunicationControl

        :param control_type: Service subfunction. Allowed values are from 0 to 0x7F
        :type control_type: int

        :param communication_type: The communication type requested.
        :type communication_type: :ref:`CommunicationType <CommunicationType>`, int, bytes

        :param node_id: DTC memory identifier. This value is user defined and introduced in 2013 version of ISO-14229-1. 
            Possible and required only when ``control_type`` is ``enableRxAndDisableTxWithEnhancedAddressInformation`` or ``enableRxAndTxWithEnhancedAddressInformation``
            Default : ``None``
        :type node_id: int

        :param standard_version: The version of the ISO-14229 (the year). eg. 2006, 2013, 2020
        :type standard_version: int

        :raises ValueError: If parameters are out of range, missing or wrong type
        """
        tools.validate_int(control_type, min=0, max=0x7F, name='Control type')

        require_node_id = standard_version >= 2013 and control_type in (
            CommunicationControl.ControlType.enableRxAndDisableTxWithEnhancedAddressInformation,
            CommunicationControl.ControlType.enableRxAndTxWithEnhancedAddressInformation
        )

        if require_node_id and node_id is None:
            raise ValueError(
                "node_id is required when the standard version is 2013 (or more recent) and when control_type is enableRxAndDisableTxWithEnhancedAddressInformation or enableRxAndTxWithEnhancedAddressInformation ")
        elif not require_node_id and node_id is not None:
            raise ValueError(
                "node_id is only possible when the standard version is 2013 (or more recent) and when control_type is enableRxAndDisableTxWithEnhancedAddressInformation or enableRxAndTxWithEnhancedAddressInformation ")

        communication_type = cls.normalize_communication_type(communication_type)
        request = Request(service=cls, subfunction=control_type)
        payload = communication_type.get_byte()

        if node_id is not None:
            tools.validate_int(node_id, min=0, max=0xFFFF, name='nodeIdentificationNumber')
            payload += struct.pack('>H', node_id)

        request.data = payload
        return request

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        """
        Populates the response ``service_data`` property with an instance of :class:`CommunicationControl.ResponseData<udsoncan.services.CommunicationControl.ResponseData>`

        :param response: The received response to interpret
        :type response: :ref:`Response<Response>`

        :raises InvalidResponseException: If length of ``response.data`` is too short
        """
        if response.data is None:
            raise InvalidResponseException(response, "No data in response")

        if len(response.data) < 1:
            raise InvalidResponseException(response, "Response data must be at least 1 byte")

        response.service_data = cls.ResponseData(
            control_type_echo=response.data[0]
        )

        return cast(CommunicationControl.InterpretedResponse, response)
