from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
from udsoncan.exceptions import *


class SecuredDataTransmission(BaseService):
    _sid = 0x84

    class Code:
        GeneralSecurityViolation = ResponseCode.GeneralSecurityViolation - 0x38
        SecuredModeRequested = ResponseCode.SecuredModeRequested - 0x38
        InsufficientProtection = ResponseCode.InsufficientProtection - 0x38
        TerminationWithSignatureRequested = ResponseCode.TerminationWithSignatureRequested - 0x38
        AccessDenied = ResponseCode.AccessDenied - 0x38
        VersionNotSupported = ResponseCode.VersionNotSupported - 0x38
        SecuredLinkNotSupported = ResponseCode.SecuredLinkNotSupported - 0x38
        CertificateNotAvailable = ResponseCode.CertificateNotAvailable - 0x38
        AuditTrailInformationNotAvailable = ResponseCode.AuditTrailInformationNotAvailable - 0x38

    supported_negative_response = [ResponseCode.SubFunctionNotSupported,
                                   ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.GeneralSecurityViolation,
                                   ResponseCode.SecuredModeRequested,
                                   ResponseCode.InsufficientProtection,
                                   ResponseCode.TerminationWithSignatureRequested,
                                   ResponseCode.AccessDenied,
                                   ResponseCode.VersionNotSupported,
                                   ResponseCode.SecuredLinkNotSupported,
                                   ResponseCode.CertificateNotAvailable,
                                   ResponseCode.AuditTrailInformationNotAvailable
                                   ]

    class ResponseData(BaseResponseData):
        def __init__(self):
            super().__init__(SecuredDataTransmission)

    class InterpretedResponse(Response):
        service_data: "SecuredDataTransmission.ResponseData"

    @classmethod
    def make_request(cls) -> Request:
        raise NotImplementedError('Service is not implemented')

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        raise NotImplementedError('Service is not implemented')
