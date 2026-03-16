from udsoncan.BaseService import BaseService
import inspect
import struct

from typing import Type, Optional, Union


class Request:
    """
    Represents a UDS Request.

    :param service: The service for which to make the request. This parameter must be a class that extends :class:`udsoncan.services.BaseService`
    :type service: class

    :param subfunction: The service subfunction. This value may be ignored if the given service does not supports subfunctions
    :type subfunction: int or None

    :param suppress_positive_response: Indicates that the server should not send a response if the response code is positive. 
            This parameter has effect only when the given service supports subfunctions
    :type suppress_positive_response: bool

    :param data: The service data appended after service ID and payload
    :type data: bytes
    """
    service: Optional[Type[BaseService]]
    subfunction: Optional[int]
    data: Optional[bytes]
    suppress_positive_response: bool

    def __init__(self,
                 service: Optional[Union[BaseService, Type[BaseService]]] = None,
                 subfunction: Optional[int] = None,
                 suppress_positive_response: bool = False,
                 data: Optional[bytes] = None):
        if service is None:
            self.service = None
        elif isinstance(service, BaseService):
            self.service = service.__class__
        elif inspect.isclass(service) and issubclass(service, BaseService):
            self.service = service
        elif service is not None:
            raise ValueError("Given service must be a service class or instance")

        if not isinstance(suppress_positive_response, bool):
            raise ValueError("suppress_positive_response must be a boolean value")

        if subfunction is not None:
            if isinstance(subfunction, int):
                self.subfunction = subfunction
            else:
                raise ValueError("Given subfunction must be a valid integer")
        else:
            self.subfunction = None

        if self.service is not None:
            if suppress_positive_response and self.service.use_subfunction() == False:
                raise ValueError('Cannot suppress positive response for service %s. This service does not have a subfunction' %
                                 (self.service.get_name()))
        self.suppress_positive_response = suppress_positive_response

        if data is not None and not isinstance(data, bytes):
            raise ValueError("data must be a valid bytes object")

        self.data = data

    def get_payload(self, suppress_positive_response: Optional[bool] = None) -> bytes:
        """
        Generates a payload to be given to the underlying protocol.
        This method is meant to be used by a UDS client

        :return: A payload to be sent through the underlying protocol
        :rtype: bytes
        """
        if self.service is None:
            raise ValueError("Cannot generate a payload. Service not set")

        if not issubclass(self.service, BaseService):
            raise ValueError("Cannot generate a payload. Given service is not a subclass of BaseService")
        use_subfunction = self.service.use_subfunction()
        if use_subfunction and not isinstance(self.subfunction, int):
            raise ValueError("Cannot generate a payload. Given subfunction is not a valid integer")

        requestid = self.service.request_id()  # Returns the service ID used to make a client request

        payload = struct.pack("B", requestid)
        if use_subfunction:
            assert self.subfunction is not None
            subfunction = self.subfunction
            if suppress_positive_response is None:
                if self.suppress_positive_response:
                    subfunction |= 0x80
            else:
                if suppress_positive_response == True:
                    subfunction |= 0x80
                elif suppress_positive_response == False:
                    subfunction &= ~0x80
            payload += struct.pack("B", subfunction)
        else:
            if suppress_positive_response == True or self.suppress_positive_response == True:
                raise ValueError('Cannot suppress positive response for service %s. This service does not have a subfunction' %
                                 (self.service.get_name()))

        if self.data is not None:
            payload += self.data

        return payload

    @classmethod
    def from_payload(cls, payload: bytes) -> "Request":
        """
        Creates a ``Request`` object from a payload coming from the underlying protocols.
        This method is meant to be used by a UDS server

        :param payload: The payload of data to parse
        :type payload: bytes

        :return: A :ref:`Request<Request>` object with populated fields
        :rtype: :ref:`Request<Request>`
        """
        req = cls()

        if len(payload) >= 1:
            req.service = BaseService.from_request_id(payload[0])
            if req.service is not None:		# Invalid service ID will make service None
                offset = 0
                if req.service.use_subfunction():
                    offset += 1
                    if len(payload) >= offset + 1:
                        req.subfunction = int(payload[1]) & 0x7F
                        req.suppress_positive_response = True if payload[1] & 0x80 > 0 else False
                if len(payload) > offset + 1:
                    req.data = payload[offset + 1:]
        return req

    def __repr__(self) -> str:
        suppress_positive_response = '[SuppressPosResponse] ' if self.suppress_positive_response else ''
        service_name = 'NoService'
        if self.service is not None:
            service_name = self.service.get_name()
        subfunction_name = ''
        if self.subfunction is not None and (self.service is None or self.service is not None and self.service.use_subfunction()):
            subfunction_name = '(subfunction=%d) ' % self.subfunction
        bytesize = len(self.data) if self.data is not None else 0
        return '<Request: [%s] %s- %d data bytes %sat 0x%08x>' % (service_name, subfunction_name, bytesize, suppress_positive_response, id(self))

    def __len__(self) -> int:
        try:
            return len(self.get_payload())
        except:
            return 0
