from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.BaseService import BaseService, BaseResponseData
from udsoncan.ResponseCode import ResponseCode
from udsoncan.exceptions import *


class ReadScalingDataByIdentifier(BaseService):
    _sid = 0x24

    supported_negative_response = [ResponseCode.IncorrectMessageLengthOrInvalidFormat,
                                   ResponseCode.ConditionsNotCorrect,
                                   ResponseCode.RequestOutOfRange,
                                   ResponseCode.SecurityAccessDenied
                                   ]

    class ResponseData(BaseResponseData):
        def __init__(self):
            super().__init__(ReadScalingDataByIdentifier)

    class InterpretedResponse(Response):
        service_data: "ReadScalingDataByIdentifier.ResponseData"

    @classmethod
    def make_request(cls) -> Request:
        raise NotImplementedError('Service is not implemented')

    @classmethod
    def interpret_response(cls, response: Response) -> InterpretedResponse:
        raise NotImplementedError('Service is not implemented')
