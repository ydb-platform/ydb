import attr

from openapi_core.exceptions import OpenAPIError


class ResponseFinderError(OpenAPIError):
    """Response finder error"""


@attr.s(hash=True)
class ResponseNotFound(ResponseFinderError):
    """Find response error"""
    http_status = attr.ib()
    responses = attr.ib()

    def __str__(self):
        return "Unknown response http status: {0}".format(
            str(self.http_status))
