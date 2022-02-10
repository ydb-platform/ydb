class CloudFunctionException(Exception):
    error = "Generic exception"

    def __init__(self, reason: str):
        self.reason = reason


class ConnectionFailure(CloudFunctionException):
    error = "Connection failure"


class ValidationError(CloudFunctionException):
    error = "Incoming parameters invalid"
