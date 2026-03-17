import inspect

from udsoncan.ResponseCode import ResponseCode
from abc import ABC

from typing import Type, List, Optional


class BaseSubfunction:

    @classmethod
    def get_name(cls, subfn_id: int) -> str:
        attributes = inspect.getmembers(cls, lambda a: not (inspect.isroutine(a)))
        subfn_list = [a for a in attributes if not (a[0].startswith('__') and a[0].endswith('__'))]

        for subfn in subfn_list:
            if isinstance(subfn[1], int):
                if subfn[1] == subfn_id:  # [1] is value
                    return subfn[0] 		# [0] is property name
            elif isinstance(subfn[1], tuple):
                if subfn_id >= subfn[1][0] or subfn_id <= subfn[1][1]:
                    return subfn[0]
        name = cls.__name__ if not hasattr(cls, '__pretty_name__') else cls.__pretty_name__
        return 'Custom %s' % name


class BaseService(ABC):

    always_valid_negative_response = [
        ResponseCode.GeneralReject,
        ResponseCode.ServiceNotSupported,
        ResponseCode.ResponseTooLong,
        ResponseCode.BusyRepeatRequest,
        ResponseCode.NoResponseFromSubnetComponent,
        ResponseCode.FailurePreventsExecutionOfRequestedAction,
        ResponseCode.SecurityAccessDenied,  # ISO-14229:2006 Table A.1:  "Besides the mandatory use of this negative response code as specified in the applicable services within ISO 14229, this negative response code can also be used for any case where security is required and is not yet granted to perform the required service."
        ResponseCode.AuthenticationRequired,  # ISO-14229:2020 Figure 5 - General server response behaviour
        ResponseCode.SecureDataTransmissionRequired,  # ISO-14229:2020 Figure 5 - General server response behaviour
        ResponseCode.SecureDataTransmissionNotAllowed,  # ISO-14229:2020 Figure 5 - General server response behaviour
        ResponseCode.RequestCorrectlyReceived_ResponsePending,
        ResponseCode.ServiceNotSupportedInActiveSession,
        ResponseCode.ResourceTemporarilyNotAvailable
    ]

    _sid: int
    _use_subfunction: bool
    supported_negative_response: List[int]

    @classmethod  # Returns the service ID used for a client request
    def request_id(cls) -> int:
        return cls._sid

    @classmethod  # Returns the service ID used for a server response
    def response_id(cls) -> int:
        return cls._sid + 0x40

    @staticmethod
    def __get_all_subclasses(cls) -> List[Type["BaseService"]]:
        import udsoncan.services    # This import is required for __subclasses__ to have a value. Python never import twice the same package.

        # this gets all subclasses and returns a list where the "most original" subclasses are listed in front of the other subclasses
        # This enables subclasses of any BaseService outside of udsoncan.services to be available, enabling specialization of calls in
        # cases where a CAN message is similar to one found in official UDS documentation but has a different service ID
        # This also allows for custom UDS service creation where a nonstandard extension is more easily played ontop of the protocol
        lst = []
        lst.extend(cls.__subclasses__())
        for x in cls.__subclasses__():
            subclasses = BaseService.__get_all_subclasses(x)
            if len(subclasses) > 0:
                lst.extend(subclasses)
        return lst

    @classmethod  # Returns an instance of the service identified by the service ID (Request)
    def from_request_id(cls, given_id: int) -> Optional[Type["BaseService"]]:
        classes = BaseService.__get_all_subclasses(cls)
        for obj in classes:
            if obj.request_id() == given_id:
                return obj
        return None

    @classmethod  # Returns an instance of the service identified by the service ID (Response)
    def from_response_id(cls, given_id: int) -> Optional[Type["BaseService"]]:
        classes = BaseService.__get_all_subclasses(cls)
        for obj in classes:
            if obj.response_id() == int(given_id):
                return obj
        return None

    # Default subfunction ID for service that does not implement subfunction_id().
    def subfunction_id(self) -> int:
        return 0

    @classmethod  # Tells if this service includes a subfunction byte
    def use_subfunction(cls) -> bool:
        if hasattr(cls, '_use_subfunction'):
            return cls._use_subfunction
        else:
            return True

    @classmethod
    def has_response_data(cls) -> bool:
        if hasattr(cls, '_no_response_data'):
            return False if cls._no_response_data else True
        else:
            return True

    @classmethod  # Returns the service name. Shortcut that works on class and instances
    def get_name(cls) -> str:
        return cls.__name__

    @classmethod  # Tells if the given response code is expected for this service according to UDS standard.
    def is_supported_negative_response(cls, code: int) -> bool:
        supported = False
        if code in cls.supported_negative_response:
            supported = True

        if code in cls.always_valid_negative_response:
            supported = True

        # As specified by Annex A, negative response code ranging above 0x7F can be used anytime if the service can return ConditionNotCorrect
        if code >= 0x80 and code < 0xFF and ResponseCode.ConditionsNotCorrect in cls.supported_negative_response:
            supported = True

        # ISO-14229:2006 Table A.1 : "This response code shall be supported by each diagnostic service with a subfunction parameter"
        if code == ResponseCode.SubFunctionNotSupportedInActiveSession and cls.use_subfunction():
            supported = True

        return supported


class BaseResponseData:
    def __init__(self, service_class):
        if not issubclass(service_class, BaseService):
            raise ValueError('service_class must be a service class')

        self.service_class = service_class

    def __repr__(self):
        return '<%s (%s) at 0x%08x>' % (self.__class__.__name__, self.service_class.__name__, id(self))
