from openapi_core.deserializing.media_types.util import (
    json_loads, urlencoded_form_loads, data_form_loads,
)

from openapi_core.deserializing.media_types.deserializers import (
    PrimitiveDeserializer,
)


class MediaTypeDeserializersFactory(object):

    MEDIA_TYPE_DESERIALIZERS = {
        'application/json': json_loads,
        'application/x-www-form-urlencoded': urlencoded_form_loads,
        'multipart/form-data': data_form_loads,
    }

    def __init__(self, custom_deserializers=None):
        if custom_deserializers is None:
            custom_deserializers = {}
        self.custom_deserializers = custom_deserializers

    def create(self, mimetype):
        deserialize_callable = self.get_deserializer_callable(
            mimetype)
        return PrimitiveDeserializer(
            mimetype, deserialize_callable)

    def get_deserializer_callable(self, mimetype):
        if mimetype in self.custom_deserializers:
            return self.custom_deserializers[mimetype]
        return self.MEDIA_TYPE_DESERIALIZERS.get(mimetype, lambda x: x)
