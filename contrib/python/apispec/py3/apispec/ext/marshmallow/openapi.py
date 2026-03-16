"""Utilities for generating OpenAPI Specification (fka Swagger) entities from
marshmallow :class:`Schemas <marshmallow.Schema>` and :class:`Fields <marshmallow.fields.Field>`.

.. warning::

    This module is treated as private API.
    Users should not need to use this module directly.
"""
from collections import OrderedDict

import marshmallow
from marshmallow.utils import is_collection

from apispec.utils import OpenAPIVersion, build_reference
from apispec.exceptions import APISpecError
from .field_converter import FieldConverterMixin
from .common import (
    get_fields,
    make_schema_key,
    resolve_schema_instance,
    get_unique_schema_name,
)


MARSHMALLOW_VERSION_INFO = tuple(
    [int(part) for part in marshmallow.__version__.split(".") if part.isdigit()]
)


__location_map__ = {
    "match_info": "path",
    "query": "query",
    "querystring": "query",
    "json": "body",
    "headers": "header",
    "cookies": "cookie",
    "form": "formData",
    "files": "formData",
}


class OpenAPIConverter(FieldConverterMixin):
    """Adds methods for generating OpenAPI specification from marshmallow schemas and fields.

    :param str|OpenAPIVersion openapi_version: The OpenAPI version to use.
        Should be in the form '2.x' or '3.x.x' to comply with the OpenAPI standard.
    :param callable schema_name_resolver: Callable to generate the schema definition name.
        Receives the `Schema` class and returns the name to be used in refs within
        the generated spec. When working with circular referencing this function
        must must not return `None` for schemas in a circular reference chain.
    :param APISpec spec: An initalied spec. Nested schemas will be added to the
        spec
    """

    def __init__(self, openapi_version, schema_name_resolver, spec):
        self.openapi_version = OpenAPIVersion(openapi_version)
        self.schema_name_resolver = schema_name_resolver
        self.spec = spec
        self.init_attribute_functions()
        # Schema references
        self.refs = {}

    @staticmethod
    def _observed_name(field, name):
        """Adjust field name to reflect `dump_to` and `load_from` attributes.

        :param Field field: A marshmallow field.
        :param str name: Field name
        :rtype: str
        """
        if MARSHMALLOW_VERSION_INFO[0] < 3:
            # use getattr in case we're running against older versions of marshmallow.
            dump_to = getattr(field, "dump_to", None)
            load_from = getattr(field, "load_from", None)
            return dump_to or load_from or name
        return field.data_key or name

    def resolve_nested_schema(self, schema):
        """Return the OpenAPI representation of a marshmallow Schema.

        Adds the schema to the spec if it isn't already present.

        Typically will return a dictionary with the reference to the schema's
        path in the spec unless the `schema_name_resolver` returns `None`, in
        which case the returned dictoinary will contain a JSON Schema Object
        representation of the schema.

        :param schema: schema to add to the spec
        """
        schema_instance = resolve_schema_instance(schema)
        schema_key = make_schema_key(schema_instance)
        if schema_key not in self.refs:
            name = self.schema_name_resolver(schema)
            if not name:
                try:
                    json_schema = self.schema2jsonschema(schema_instance)
                except RuntimeError:
                    raise APISpecError(
                        "Name resolver returned None for schema {schema} which is "
                        "part of a chain of circular referencing schemas. Please"
                        " ensure that the schema_name_resolver passed to"
                        " MarshmallowPlugin returns a string for all circular"
                        " referencing schemas.".format(schema=schema)
                    )
                if getattr(schema, "many", False):
                    return {"type": "array", "items": json_schema}
                return json_schema
            name = get_unique_schema_name(self.spec.components, name)
            self.spec.components.schema(name, schema=schema)
        return self.get_ref_dict(schema_instance)

    def schema2parameters(
        self,
        schema,
        *,
        default_in="body",
        name="body",
        required=False,
        description=None
    ):
        """Return an array of OpenAPI parameters given a given marshmallow
        :class:`Schema <marshmallow.Schema>`. If `default_in` is "body", then return an array
        of a single parameter; else return an array of a parameter for each included field in
        the :class:`Schema <marshmallow.Schema>`.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.2.md#parameterObject
        """
        openapi_default_in = __location_map__.get(default_in, default_in)
        if self.openapi_version.major < 3 and openapi_default_in == "body":
            prop = self.resolve_nested_schema(schema)

            param = {
                "in": openapi_default_in,
                "required": required,
                "name": name,
                "schema": prop,
            }

            if description:
                param["description"] = description

            return [param]

        assert not getattr(
            schema, "many", False
        ), "Schemas with many=True are only supported for 'json' location (aka 'in: body')"

        fields = get_fields(schema, exclude_dump_only=True)

        return self.fields2parameters(fields, default_in=default_in)

    def fields2parameters(self, fields, *, default_in):
        """Return an array of OpenAPI parameters given a mapping between field names and
        :class:`Field <marshmallow.Field>` objects. If `default_in` is "body", then return an array
        of a single parameter; else return an array of a parameter for each included field in
        the :class:`Schema <marshmallow.Schema>`.

        In OpenAPI3, only "query", "header", "path" or "cookie" are allowed for the location
        of parameters. In OpenAPI 3, "requestBody" is used when fields are in the body.

        This function always returns a list, with a parameter
        for each included field in the :class:`Schema <marshmallow.Schema>`.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.2.md#parameterObject
        """
        parameters = []
        body_param = None
        for field_name, field_obj in fields.items():
            if field_obj.dump_only:
                continue
            param = self.field2parameter(
                field_obj,
                name=self._observed_name(field_obj, field_name),
                default_in=default_in,
            )
            if (
                self.openapi_version.major < 3
                and param["in"] == "body"
                and body_param is not None
            ):
                body_param["schema"]["properties"].update(param["schema"]["properties"])
                required_fields = param["schema"].get("required", [])
                if required_fields:
                    body_param["schema"].setdefault("required", []).extend(
                        required_fields
                    )
            else:
                if self.openapi_version.major < 3 and param["in"] == "body":
                    body_param = param
                parameters.append(param)
        return parameters

    def field2parameter(self, field, *, name, default_in):
        """Return an OpenAPI parameter as a `dict`, given a marshmallow
        :class:`Field <marshmallow.Field>`.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.2.md#parameterObject
        """
        location = field.metadata.get("location", None)
        prop = self.field2property(field)
        return self.property2parameter(
            prop,
            name=name,
            required=field.required,
            multiple=isinstance(field, marshmallow.fields.List),
            location=location,
            default_in=default_in,
        )

    def property2parameter(
        self, prop, *, name, required, multiple, location, default_in
    ):
        """Return the Parameter Object definition for a JSON Schema property.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.2.md#parameterObject

        :param dict prop: JSON Schema property
        :param str name: Field name
        :param bool required: Parameter is required
        :param bool multiple: Parameter is repeated
        :param str location: Location to look for ``name``
        :param str default_in: Default location to look for ``name``
        :raise: TranslationError if arg object cannot be translated to a Parameter Object schema.
        :rtype: dict, a Parameter Object
        """
        openapi_default_in = __location_map__.get(default_in, default_in)
        openapi_location = __location_map__.get(location, openapi_default_in)
        ret = {"in": openapi_location, "name": name}

        if openapi_location == "body":
            ret["required"] = False
            ret["name"] = "body"
            ret["schema"] = {"type": "object", "properties": {name: prop}}
            if required:
                ret["schema"]["required"] = [name]
        else:
            ret["required"] = required
            if self.openapi_version.major < 3:
                if multiple:
                    ret["collectionFormat"] = "multi"
                ret.update(prop)
            else:
                if multiple:
                    ret["explode"] = True
                    ret["style"] = "form"
                if prop.get("description", None):
                    ret["description"] = prop.pop("description")
                ret["schema"] = prop
        return ret

    def schema2jsonschema(self, schema):
        """Return the JSON Schema Object for a given marshmallow
        :class:`Schema <marshmallow.Schema>` instance. Schema may optionally
        provide the ``title`` and ``description`` class Meta options.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.2.md#schemaObject

        :param Schema schema: A marshmallow Schema instance
        :rtype: dict, a JSON Schema Object
        """
        fields = get_fields(schema)
        Meta = getattr(schema, "Meta", None)
        partial = getattr(schema, "partial", None)
        ordered = getattr(schema, "ordered", False)

        jsonschema = self.fields2jsonschema(fields, partial=partial, ordered=ordered)

        if hasattr(Meta, "title"):
            jsonschema["title"] = Meta.title
        if hasattr(Meta, "description"):
            jsonschema["description"] = Meta.description

        return jsonschema

    def fields2jsonschema(self, fields, *, ordered=False, partial=None):
        """Return the JSON Schema Object given a mapping between field names and
        :class:`Field <marshmallow.Field>` objects.

        :param dict fields: A dictionary of field name field object pairs
        :param bool ordered: Whether to preserve the order in which fields were declared
        :param bool|tuple partial: Whether to override a field's required flag.
            If `True` no fields will be set as required. If an iterable fields
            in the iterable will not be marked as required.
        :rtype: dict, a JSON Schema Object
        """
        jsonschema = {"type": "object", "properties": OrderedDict() if ordered else {}}

        for field_name, field_obj in fields.items():
            observed_field_name = self._observed_name(field_obj, field_name)
            property = self.field2property(field_obj)
            jsonschema["properties"][observed_field_name] = property

            if field_obj.required:
                if not partial or (
                    is_collection(partial) and field_name not in partial
                ):
                    jsonschema.setdefault("required", []).append(observed_field_name)

        if "required" in jsonschema:
            jsonschema["required"].sort()

        return jsonschema

    def get_ref_dict(self, schema):
        """Method to create a dictionary containing a JSON reference to the
        schema in the spec
        """
        schema_key = make_schema_key(schema)
        ref_schema = build_reference(
            "schema", self.openapi_version.major, self.refs[schema_key]
        )
        if getattr(schema, "many", False):
            return {"type": "array", "items": ref_schema}
        return ref_schema
