import re
from typing import MutableMapping, Optional

from django.utils.module_loading import import_string


def camelize_serializer_fields(result, generator, request, public):
    from django.conf import settings
    from djangorestframework_camel_case.settings import api_settings
    from djangorestframework_camel_case.util import camelize_re, underscore_to_camel

    # prunes subtrees from camelization based on owning field name
    ignore_fields = api_settings.JSON_UNDERSCOREIZE.get("ignore_fields") or ()
    # ignore certain field names while camelizing
    ignore_keys = api_settings.JSON_UNDERSCOREIZE.get("ignore_keys") or ()

    def has_middleware_installed():
        try:
            from djangorestframework_camel_case.middleware import CamelCaseMiddleWare
        except ImportError:
            return False

        return any(
            isinstance(m, type) and issubclass(m, CamelCaseMiddleWare)
            for m in map(import_string, settings.MIDDLEWARE)
        )

    def camelize_str(key: str) -> str:
        new_key = re.sub(camelize_re, underscore_to_camel, key) if "_" in key else key
        if key in ignore_keys or new_key in ignore_keys:
            return key
        return new_key

    def camelize_component(schema: MutableMapping, name: Optional[str] = None) -> MutableMapping:
        if name is not None and (name in ignore_fields or camelize_str(name) in ignore_fields):
            return schema
        elif schema.get('type') == 'object':
            if 'properties' in schema:
                schema['properties'] = {
                    camelize_str(field_name): camelize_component(field_schema, field_name)
                    for field_name, field_schema in schema['properties'].items()
                }
            if 'required' in schema:
                schema['required'] = [camelize_str(field) for field in schema['required']]
        elif schema.get('type') == 'array' and isinstance(schema['items'], MutableMapping):
            camelize_component(schema['items'])
        return schema

    for (_, component_type), component in generator.registry._components.items():
        if component_type == 'schemas':
            camelize_component(component.schema)

    if has_middleware_installed():
        for url_schema in result["paths"].values():
            for method_schema in url_schema.values():
                for parameter in method_schema.get("parameters", []):
                    parameter["name"] = camelize_str(parameter["name"])

    # inplace modification of components also affect result dict, so regeneration is not necessary
    return result
