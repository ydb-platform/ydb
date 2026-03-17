"""OpenAPI core validation validators module"""
from __future__ import division

from openapi_core.unmarshalling.schemas.util import build_format_checker


class BaseValidator(object):

    def __init__(
            self, spec,
            base_url=None,
            custom_formatters=None, custom_media_type_deserializers=None,
    ):
        self.spec = spec
        self.base_url = base_url
        self.custom_formatters = custom_formatters or {}
        self.custom_media_type_deserializers = custom_media_type_deserializers

        self.format_checker = build_format_checker(**self.custom_formatters)

    def _find_path(self, request):
        from openapi_core.templating.paths.finders import PathFinder
        finder = PathFinder(self.spec, base_url=self.base_url)
        return finder.find(request)

    def _get_media_type(self, content, request_or_response):
        from openapi_core.templating.media_types.finders import MediaTypeFinder
        finder = MediaTypeFinder(content)
        return finder.find(request_or_response)

    def _deserialise_data(self, mimetype, value):
        from openapi_core.deserializing.media_types.factories import (
            MediaTypeDeserializersFactory,
        )
        deserializers_factory = MediaTypeDeserializersFactory(
            self.custom_media_type_deserializers)
        deserializer = deserializers_factory.create(mimetype)
        return deserializer(value)

    def _cast(self, param_or_media_type, value):
        # return param_or_media_type.cast(value)
        if 'schema' not in param_or_media_type:
            return value

        from openapi_core.casting.schemas.factories import SchemaCastersFactory
        casters_factory = SchemaCastersFactory()
        schema = param_or_media_type / 'schema'
        caster = casters_factory.create(schema)
        return caster(value)

    def _unmarshal(self, param_or_media_type, value, context):
        if 'schema' not in param_or_media_type:
            return value

        from openapi_core.unmarshalling.schemas.factories import (
            SchemaUnmarshallersFactory,
        )
        spec_resolver = self.spec.accessor.dereferencer.resolver_manager.\
            resolver
        unmarshallers_factory = SchemaUnmarshallersFactory(
            spec_resolver, self.format_checker,
            self.custom_formatters, context=context,
        )
        schema = param_or_media_type / 'schema'
        unmarshaller = unmarshallers_factory.create(schema)
        return unmarshaller(value)
