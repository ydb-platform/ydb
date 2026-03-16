import attr

from openapi_core.exceptions import OpenAPIError


@attr.s(hash=True)
class DeserializeError(OpenAPIError):
    """Deserialize operation error"""
    value = attr.ib()
    style = attr.ib()

    def __str__(self):
        return "Failed to deserialize value {value} with style {style}".format(
            value=self.value, style=self.style)
