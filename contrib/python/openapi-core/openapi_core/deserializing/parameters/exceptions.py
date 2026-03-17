import attr

from openapi_core.deserializing.exceptions import DeserializeError


@attr.s(hash=True)
class EmptyParameterValue(DeserializeError):
    name = attr.ib()

    def __str__(self):
        return "Value of parameter cannot be empty: {0}".format(self.name)
