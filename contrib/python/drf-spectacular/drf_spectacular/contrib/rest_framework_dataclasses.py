from typing import Any

from drf_spectacular.drainage import get_override, has_override
from drf_spectacular.extensions import OpenApiSerializerExtension
from drf_spectacular.plumbing import ComponentIdentity, get_doc
from drf_spectacular.utils import Direction


class OpenApiDataclassSerializerExtensions(OpenApiSerializerExtension):
    target_class = "rest_framework_dataclasses.serializers.DataclassSerializer"
    match_subclasses = True

    def get_name(self):
        """Use the dataclass name in the schema, instead of the serializer prefix (which can be just Dataclass)."""
        if has_override(self.target, 'component_name'):
            return get_override(self.target, 'component_name')
        if getattr(getattr(self.target, 'Meta', None), 'ref_name', None) is not None:
            return self.target.Meta.ref_name
        if has_override(self.target.dataclass_definition.dataclass_type, 'component_name'):
            return get_override(self.target.dataclass_definition.dataclass_type, 'component_name')
        return self.target.dataclass_definition.dataclass_type.__name__

    def get_identity(self, auto_schema, direction: Direction) -> Any:
        return ComponentIdentity(self.target.dataclass_definition.dataclass_type)

    def strip_library_doc(self, schema):
        """Strip the DataclassSerializer library documentation from the schema."""
        from rest_framework_dataclasses.serializers import DataclassSerializer
        if 'description' in schema and schema['description'] == get_doc(DataclassSerializer):
            del schema['description']
        return schema

    def map_serializer(self, auto_schema, direction: Direction):
        """"Generate the schema for a DataclassSerializer."""
        schema = auto_schema._map_serializer(self.target, direction, bypass_extensions=True)
        return self.strip_library_doc(schema)
