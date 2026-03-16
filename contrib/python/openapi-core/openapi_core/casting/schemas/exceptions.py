import attr

from openapi_core.exceptions import OpenAPIError


@attr.s(hash=True)
class CastError(OpenAPIError):
    """Schema cast operation error"""
    value = attr.ib()
    type = attr.ib()

    def __str__(self):
        return "Failed to cast value {value} to type {type}".format(
            value=self.value, type=self.type)
