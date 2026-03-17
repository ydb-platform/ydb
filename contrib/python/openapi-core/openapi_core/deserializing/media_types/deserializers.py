from openapi_core.deserializing.exceptions import DeserializeError


class PrimitiveDeserializer(object):

    def __init__(self, mimetype, deserializer_callable):
        self.mimetype = mimetype
        self.deserializer_callable = deserializer_callable

    def __call__(self, value):
        try:
            return self.deserializer_callable(value)
        except (ValueError, TypeError, AttributeError):
            raise DeserializeError(value, self.mimetype)
