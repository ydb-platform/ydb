class SellingApiException(Exception):
    """
    Generic Exception

    Parameters:

        message: str The error message
        amzn_code: str Amazon Error Code
        error: list Amazon Error list

    """
    code = 999

    def __init__(self, error, headers):
        try:
            self.message = error[0].get('message')
            self.amzn_code = error[0].get('code')
        except IndexError:
            pass
        self.error = error
        self.headers = headers


class SellingApiBadRequestException(SellingApiException):
    """
    400	Request has missing or invalid parameters and cannot be parsed.
    """
    code = 400

    def __init__(self, error, headers=None):
        super(SellingApiBadRequestException, self).__init__(error, headers)


class SellingApiForbiddenException(SellingApiException):
    """
    403	Indicates access to the resource is forbidden. Possible reasons include Access Denied, Unauthorized, Expired Token, or Invalid Signature.
    """
    code = 403

    def __init__(self, error, headers=None):
        super(SellingApiForbiddenException, self).__init__(error, headers)


class SellingApiNotFoundException(SellingApiException):
    """
    404	The resource specified does not exist.
    """
    code = 404

    def __init__(self, error, headers=None):
        super(SellingApiNotFoundException, self).__init__(error, headers)


class SellingApiStateConflictException(SellingApiException):
    """
    409	The resource specified conflicts with the current state.
    """
    code = 409

    def __init__(self, error, headers=None):
        super(SellingApiStateConflictException, self).__init__(error, headers)


class SellingApiTooLargeException(SellingApiException):
    """
    413	The request size exceeded the maximum accepted size.
    """
    code = 413

    def __init__(self, error, headers=None):
        super(SellingApiTooLargeException, self).__init__(error, headers)


class SellingApiUnsupportedFormatException(SellingApiException):
    """
    415	The request payload is in an unsupported format.
    """
    code = 415

    def __init__(self, error, headers=None):
        super(SellingApiUnsupportedFormatException, self).__init__(error, headers)


class SellingApiRequestThrottledException(SellingApiException):
    """
    429	The frequency of requests was greater than allowed.
    """
    code = 429

    def __init__(self, error, headers=None):
        super(SellingApiRequestThrottledException, self).__init__(error, headers)


class SellingApiServerException(SellingApiException):
    """
    500	An unexpected condition occurred that prevented the server from fulfilling the request.
    """
    code = 500

    def __init__(self, error, headers=None):
        super(SellingApiServerException, self).__init__(error, headers)


class SellingApiTemporarilyUnavailableException(SellingApiException):
    """
    503	Temporary overloading or maintenance of the server.
    """
    code = 503

    def __init__(self, error, headers=None):
        super(SellingApiTemporarilyUnavailableException, self).__init__(error, headers)


class SellingApiGatewayTimeoutException(SellingApiException):
    """
    503	Temporary overloading or maintenance of the server.
    """
    code = 504

    def __init__(self, error, headers=None):
        super(SellingApiGatewayTimeoutException, self).__init__(error, headers)


class MissingScopeException(Exception):
    pass


def get_exception_for_code(code: int):
    return {
        400: SellingApiBadRequestException,
        403: SellingApiForbiddenException,
        404: SellingApiNotFoundException,
        409: SellingApiStateConflictException,
        413: SellingApiTooLargeException,
        415: SellingApiUnsupportedFormatException,
        429: SellingApiRequestThrottledException,
        500: SellingApiServerException,
        503: SellingApiTemporarilyUnavailableException,
        504: SellingApiGatewayTimeoutException
    }.get(code, SellingApiException)
