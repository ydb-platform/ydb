from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
import inspect
import struct

from udsoncan.Request import Request

from typing import Type, Optional, Union


class Response:
    """
    Represents a server Response to a client Request

    :param service: The service implied by this response.
    :type service: class

    :param code: The response code
    :type code: int

    :param data: The response data encoded after the service and response code
    :type data: bytes

    .. data:: valid 

            (boolean) True if the response content is valid. Only ``invalid_reason`` is guaranteed to have a meaningful value if this value is False

    .. data:: invalid_reason 

            (string) String explaining why the response is invalid.

    .. data:: service 

            (class) The response target :ref:`service<Services>` class

    .. data:: positive 

            (boolean) True if the response code is 0 (PositiveResponse), False otherwise

    .. data:: unexpected 

            (boolean) Indicates that the response was unexpected. Set by an external source such as the :ref:`Client<Client>` object            

    .. data:: code 

            (int) The response code. 

    .. data:: code_name 

            (string) The response code name.


    .. data:: data

            (bytes) The response data. All the payload content, except the service number and the response code


    .. data:: service_data

            (object) The content of ``data`` interpreted by a service; can be any type of content.


    .. data:: original_payload 

            (bytes) When the response is built with `Response.from_payload`, this property contains a copy of the payload used. None otherwise.

    .. data:: original_request 

            (Request) Optional reference to the request object that generated this response.  """

    Code = ResponseCode

    service: Optional[Type[BaseService]]
    subfunction: Optional[int]
    data: Optional[bytes]
    suppress_positive_response: bool
    original_payload: Optional[bytes]
    service_data: Optional[BaseResponseData]
    original_request: Optional[Request]

    def __init__(self,
                 service: Optional[Union[BaseService, Type[BaseService]]] = None,
                 code: Optional[int] = None,
                 data: Optional[bytes] = None):
        if service is None:
            self.service = None
        elif isinstance(service, BaseService):
            self.service = service.__class__
        elif inspect.isclass(service) and issubclass(service, BaseService):
            self.service = service
        elif service is not None:
            raise ValueError("Given service must be a service class or instance")

        self.positive = False
        self.code = None
        self.code_name = ""
        self.valid = False
        self.invalid_reason = "Object not initialized"
        self.service_data = None
        self.original_payload = None
        self.unexpected = False
        self.original_request = None

        if data is not None:
            if not isinstance(data, bytes):
                raise ValueError("Given data must be a valid bytes object")

        self.data = data if data is not None else b''

        if code is not None:
            if not isinstance(code, int):
                raise ValueError("Response code must be a valid integer")
            elif code < 0 or code > 0xFF:
                raise ValueError("Response code must be an integer between 0 and 0xFF")
            self.code = code
            self.code_name = Response.Code.get_name(code)
            if not Response.Code.is_negative(code):
                self.positive = True

        if self.service is not None and self.code is not None:
            self.valid = True
            self.invalid_reason = ""

    # Used by server
    def get_payload(self) -> bytes:
        """
        Generates a payload to be given to the underlying protocol.
        This method is meant to be used by a UDS server

        :return: A payload to be sent through the underlying protocol
        :rtype: bytes
        """
        if self.service is None:
            raise ValueError("Cannot make payload from response object. Service is not set")

        if not isinstance(self.service, BaseService) and not issubclass(self.service, BaseService):
            raise ValueError("Cannot make payload from response object. Given service is not a valid service object")

        if not isinstance(self.code, int):
            raise ValueError("Cannot make payload from response object. Given response code is not a valid integer")

        payload = b''
        if self.positive:
            payload += struct.pack("B", self.service.response_id())
        else:
            payload += b'\x7F'
            payload += struct.pack("B", self.service.request_id())
            payload += struct.pack('B', self.code)

        if self.data is not None and self.service.has_response_data():
            payload += self.data
        return payload

    # Analyzes a TP frame and builds a Response object. Used by client

    @classmethod
    def from_payload(cls, payload: bytes) -> "Response":
        """
        Creates a ``Response`` object from a payload coming from the underlying protocol.
        This method is meant to be used by a UDS client

        :param payload: The payload of data to parse
        :type payload: bytes

        :return: A :ref:`Response<Response>` object with populated fields
        :rtype: :ref:`Response<Response>`
        """
        response = cls()
        response.original_payload = payload  # may be useful for debugging

        if len(payload) < 1:
            response.valid = False
            response.invalid_reason = "Payload is empty"
            return response

        if payload[0] != 0x7F:  # Positive
            response.service = BaseService.from_response_id(payload[0])
            if response.service is None:
                response.valid = False
                response.invalid_reason = "Payload first byte is not a know service response ID."
                return response

            data_start = 1
            response.positive = True
            if len(payload) < 2 and response.service.has_response_data():
                response.valid = False
                response.positive = False
                response.invalid_reason = "Payload must be at least 2 bytes long (service and response)"
                return response

            response.code = Response.Code.PositiveResponse
            response.code_name = Response.Code.get_name(Response.Code.PositiveResponse)

        else:  # Negative response
            response.positive = False
            data_start = 3

            if len(payload) < 2:
                response.valid = False
                response.invalid_reason = "Incomplete invalid response service (7Fxx)"
                return response
            response.service = BaseService.from_request_id(payload[1])  # Request id, not response id

            if response.service is None:
                response.valid = False
                response.invalid_reason = "Payload second byte is not a known service request ID."
                return response

            if len(payload) < 3:
                response.valid = False
                response.invalid_reason = "Response code missing"
                return response

            response.code = int(payload[2])
            response.code_name = Response.Code.get_name(response.code)

        response.valid = True
        response.invalid_reason = ""
        if len(payload) > data_start:
            response.data = payload[data_start:]
        return response

    def __repr__(self) -> str:
        responsename = Response.Code.get_name(Response.Code.PositiveResponse) if self.positive else 'NegativeResponse(%s)' % self.code_name
        bytesize = len(self.data) if self.data is not None else 0
        service_name = "NoService"
        if self.service is not None:
            service_name = self.service.get_name()
        return '<%s: [%s] - %d data bytes at 0x%08x>' % (responsename, service_name, bytesize, id(self))

    def __len__(self) -> int:
        try:
            return len(self.get_payload())
        except:
            return 0
