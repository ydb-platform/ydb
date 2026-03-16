from drf_spectacular.extensions import OpenApiSerializerFieldExtension
from drf_spectacular.plumbing import build_array_type, is_list_serializer


class RecursiveFieldExtension(OpenApiSerializerFieldExtension):
    target_class = "rest_framework_recursive.fields.RecursiveField"

    def map_serializer_field(self, auto_schema, direction):
        proxied = self.target.proxied

        if is_list_serializer(proxied):
            component = auto_schema.resolve_serializer(proxied.child, direction)
            return build_array_type(component.ref)

        component = auto_schema.resolve_serializer(proxied, direction)
        return component.ref
