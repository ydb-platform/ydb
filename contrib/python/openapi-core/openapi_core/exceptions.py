"""OpenAPI core exceptions module"""
import attr


class OpenAPIError(Exception):
    pass


class OpenAPIParameterError(OpenAPIError):
    pass


class MissingParameterError(OpenAPIParameterError):
    """Missing parameter error"""
    pass


@attr.s(hash=True)
class MissingParameter(MissingParameterError):
    name = attr.ib()

    def __str__(self):
        return "Missing parameter (without default value): {0}".format(
            self.name)


@attr.s(hash=True)
class MissingRequiredParameter(MissingParameterError):
    name = attr.ib()

    def __str__(self):
        return "Missing required parameter: {0}".format(self.name)


class OpenAPIRequestBodyError(OpenAPIError):
    pass


class MissingRequestBodyError(OpenAPIRequestBodyError):
    """Missing request body error"""
    pass


@attr.s(hash=True)
class MissingRequestBody(MissingRequestBodyError):
    request = attr.ib()

    def __str__(self):
        return "Missing request body"


@attr.s(hash=True)
class MissingRequiredRequestBody(MissingRequestBodyError):
    request = attr.ib()

    def __str__(self):
        return "Missing required request body"


class OpenAPIResponseError(OpenAPIError):
    pass


@attr.s(hash=True)
class MissingResponseContent(OpenAPIResponseError):
    response = attr.ib()

    def __str__(self):
        return "Missing response content"
