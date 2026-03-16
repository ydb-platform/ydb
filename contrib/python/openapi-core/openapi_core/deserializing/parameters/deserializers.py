from openapi_core.deserializing.exceptions import DeserializeError
from openapi_core.deserializing.parameters.exceptions import (
    EmptyParameterValue,
)
from openapi_core.schema.parameters import get_aslist, get_explode, get_style


class PrimitiveDeserializer(object):

    def __init__(self, param, deserializer_callable):
        self.param = param
        self.deserializer_callable = deserializer_callable

        self.aslist = get_aslist(self.param)
        self.explode = get_explode(self.param)
        self.style = get_style(self.param)

    def __call__(self, value):
        if (self.param['in'] == 'query' and value == "" and
                not self.param.getkey('allowEmptyValue', False)):
            raise EmptyParameterValue(
                value, self.style, self.param['name'])

        if not self.aslist or self.explode:
            return value
        try:
            return self.deserializer_callable(value)
        except (ValueError, TypeError, AttributeError):
            raise DeserializeError(value, self.style)
