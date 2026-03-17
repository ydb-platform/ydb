import attr

from openapi_core.exceptions import OpenAPIError


class PathError(OpenAPIError):
    """Path error"""


@attr.s(hash=True)
class PathNotFound(PathError):
    """Find path error"""
    url = attr.ib()

    def __str__(self):
        return "Path not found for {0}".format(self.url)


@attr.s(hash=True)
class OperationNotFound(PathError):
    """Find path operation error"""
    url = attr.ib()
    method = attr.ib()

    def __str__(self):
        return "Operation {0} not found for {1}".format(
            self.method, self.url)


@attr.s(hash=True)
class ServerNotFound(PathError):
    """Find server error"""
    url = attr.ib()

    def __str__(self):
        return "Server not found for {0}".format(self.url)
