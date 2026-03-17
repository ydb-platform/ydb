class AdvertisingApiException(Exception):
    def __init__(self, code, error, headers):
        try:
            self.message = error.get('details')
            self.amzn_code = error.get('code')
        except (IndexError, AttributeError):
            pass
        self.code = code
        self.error = error
        self.headers = headers

    def get_code(self):
        return self.code

    def get_error(self):
        return self.error

    def get_headers(self):
        return self.headers

    def get_amzn_code(self):
        return self.amzn_code

    def get_details(self):
        return self.message


class AdvertisingTypeException(Exception):
    code = 888

    def __init__(self, type, error):
        try:
            self.type = type
            self.message = error
        except IndexError:
            pass
        self.error = error


class AdvertisingApiBadRequestException(AdvertisingApiException):
    """
    400	Request has missing or invalid parameters and cannot be parsed.
    """

    code = 400

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiBadRequestException, self).__init__(code, error, headers)


class AdvertisingApiUnauthorizedException(AdvertisingApiException):
    """
    401	Unauthorized. The request failed because the user is not authenticated or is not allowed to invoke the operation.
    """

    code = 401

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiUnauthorizedException, self).__init__(code, error, headers)


class AdvertisingApiForbiddenException(AdvertisingApiException):
    """
    403	Forbidden. The request failed because user does not have access to a specified resource.
    """

    code = 403

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiForbiddenException, self).__init__(code, error, headers)


class AdvertisingApiNotFoundException(AdvertisingApiException):
    """
    404	Not Found
    """

    code = 404

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiNotFoundException, self).__init__(code, error, headers)


class AdvertisingApiUnprocessableEntityException(AdvertisingApiException):
    """
    422	Unprocessable Entity
    """

    code = 422

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiUnprocessableEntityException, self).__init__(code, error, headers)


class AdvertisingApiTooManyRequestsException(AdvertisingApiException):
    """
    429	Too Many Requests. The request was rate-limited. Retry later.
    """

    code = 429

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiTooManyRequestsException, self).__init__(code, error, headers)


class AdvertisingApiInternalServerErrorException(AdvertisingApiException):
    """
    500	Internal Server Error. Something went wrong on the server. Retry later and report an error if unresolved.
    """

    code = 500

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiInternalServerErrorException, self).__init__(code, error, headers)


class AdvertisingApiTemporarilyUnavailableException(AdvertisingApiException):
    """
    503	Temporary overloading or maintenance of the server.
    """

    code = 503

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiTemporarilyUnavailableException, self).__init__(code, error, headers)


class AdvertisingApiInvalidParameterValueException(AdvertisingApiException):
    """
    200	InvalidParameterValue
    """

    code = 200

    def __init__(self, code, error, headers=None):
        super(AdvertisingApiInvalidParameterValueException, self).__init__(code, error, headers)


def get_exception_for_code(code: int):
    return {
        400: AdvertisingApiBadRequestException,
        401: AdvertisingApiUnauthorizedException,
        403: AdvertisingApiForbiddenException,
        404: AdvertisingApiNotFoundException,
        422: AdvertisingApiUnprocessableEntityException,
        429: AdvertisingApiTooManyRequestsException,
        500: AdvertisingApiInternalServerErrorException,
        503: AdvertisingApiTemporarilyUnavailableException,
    }.get(code, AdvertisingApiException)


def get_exception_for_content(content: object):
    return {'InvalidParameterValue': AdvertisingApiInvalidParameterValueException}.get(422, AdvertisingApiException)
