from drf_spectacular.drainage import error, warn
from drf_spectacular.extensions import OpenApiSerializerExtension
from drf_spectacular.plumbing import force_instance, is_list_serializer, is_serializer


class PolymorphicProxySerializerExtension(OpenApiSerializerExtension):
    target_class = 'drf_spectacular.utils.PolymorphicProxySerializer'
    priority = -1

    def get_name(self):
        return self.target.component_name

    def map_serializer(self, auto_schema, direction):
        """ custom handling for @extend_schema's injection of PolymorphicProxySerializer """
        if isinstance(self.target.serializers, dict):
            sub_components = self._get_explicit_sub_components(auto_schema, direction)
        else:
            sub_components = self._get_implicit_sub_components(auto_schema, direction)

        if not self._has_discriminator():
            return {'oneOf': [schema for _, schema in sub_components]}
        else:
            one_of_list = []
            for _, schema in sub_components:
                if schema not in one_of_list:
                    one_of_list.append(schema)
            return {
                'oneOf': one_of_list,
                'discriminator': {
                    'propertyName': self.target.resource_type_field_name,
                    'mapping': {resource_type: schema['$ref'] for resource_type, schema in sub_components}
                }
            }

    def _get_implicit_sub_components(self, auto_schema, direction):
        sub_components = []
        for sub_serializer in self.target.serializers:
            sub_serializer = self._prep_serializer(sub_serializer)
            (resolved_name, resolved_schema) = self._process_serializer(auto_schema, sub_serializer, direction)
            if not resolved_schema:
                continue

            if not self._has_discriminator():
                sub_components.append((None, resolved_schema))
            else:
                try:
                    discriminator_field = sub_serializer.fields[self.target.resource_type_field_name]
                    resource_type = discriminator_field.to_representation(None)
                except:  # noqa: E722
                    warn(
                        f'sub-serializer {resolved_name} of {self.target.component_name} '
                        f'must contain the discriminator field "{self.target.resource_type_field_name}". '
                        f'defaulting to sub-serializer name, but schema will likely not match the API.'
                    )
                    resource_type = resolved_name
                sub_components.append((resource_type, resolved_schema))

        return sub_components

    def _get_explicit_sub_components(self, auto_schema, direction):
        sub_components = []
        for resource_type, sub_serializer in self.target.serializers.items():
            sub_serializer = self._prep_serializer(sub_serializer)
            (_, resolved_schema) = self._process_serializer(auto_schema, sub_serializer, direction)
            if resolved_schema:
                sub_components.append((resource_type, resolved_schema))

        return sub_components

    def _has_discriminator(self):
        return self.target.resource_type_field_name is not None

    def _prep_serializer(self, serializer):
        serializer = force_instance(serializer)
        serializer.partial = self.target.partial
        return serializer

    def _process_serializer(self, auto_schema, serializer, direction):
        if is_list_serializer(serializer):
            if self._has_discriminator() or self.target._many is not False:
                warn(
                    "To control sub-serializer's \"many\" attribute, following usage pattern is necessary: "
                    "PolymorphicProxySerializer(serializers=[...], resource_type_field_name=None, "
                    "many=False). Ignoring serializer as it is not processable in this configuration."
                )
                return None, None
            schema = auto_schema._unwrap_list_serializer(serializer, direction)
            return None, schema
        elif is_serializer(serializer):
            resolved = auto_schema.resolve_serializer(serializer, direction)
            return (resolved.name, resolved.ref) if resolved else (None, None)
        else:
            error("PolymorphicProxySerializer's serializer argument contained unknown objects.")
            return None, None
