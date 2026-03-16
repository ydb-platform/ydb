# -*- coding: utf-8 -*-
"""Utilities for generating OpenAPI spec (fka Swagger) entities from
marshmallow :class:`Schemas <marshmallow.Schema>` and :class:`Fields <marshmallow.fields.Field>`.

OpenAPI 2.0 spec: https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md
"""
from __future__ import absolute_import, unicode_literals
import operator
import warnings
import functools
from collections import OrderedDict
import copy

import marshmallow
from marshmallow.utils import is_collection
from marshmallow.compat import iteritems
from marshmallow.orderedset import OrderedSet

from apispec.lazy_dict import LazyDict
from apispec.utils import OpenAPIVersion
from .common import resolve_schema_cls

##### marshmallow #####

MARSHMALLOW_VERSION_INFO = tuple(
    [int(part) for part in marshmallow.__version__.split('.') if part.isdigit()],
)

# marshmallow field => (JSON Schema type, format)
DEFAULT_FIELD_MAPPING = {
    marshmallow.fields.Integer: ('integer', 'int32'),
    marshmallow.fields.Number: ('number', None),
    marshmallow.fields.Float: ('number', 'float'),
    marshmallow.fields.Decimal: ('number', None),
    marshmallow.fields.String: ('string', None),
    marshmallow.fields.Boolean: ('boolean', None),
    marshmallow.fields.UUID: ('string', 'uuid'),
    marshmallow.fields.DateTime: ('string', 'date-time'),
    marshmallow.fields.Date: ('string', 'date'),
    marshmallow.fields.Time: ('string', None),
    marshmallow.fields.Email: ('string', 'email'),
    marshmallow.fields.URL: ('string', 'url'),
    marshmallow.fields.Dict: ('object', None),
    # Assume base Field and Raw are strings
    marshmallow.fields.Field: ('string', None),
    marshmallow.fields.Raw: ('string', None),
    marshmallow.fields.List: ('array', None),
}


__location_map__ = {
    'query': 'query',
    'querystring': 'query',
    'json': 'body',
    'headers': 'header',
    'cookies': 'cookie',
    'form': 'formData',
    'files': 'formData',
}


# Properties that may be defined in a field's metadata that will be added to the output
# of field2property
# https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#schemaObject
_VALID_PROPERTIES = {
    'format',
    'title',
    'description',
    'default',
    'multipleOf',
    'maximum',
    'exclusiveMaximum',
    'minimum',
    'exclusiveMinimum',
    'maxLength',
    'minLength',
    'pattern',
    'maxItems',
    'minItems',
    'uniqueItems',
    'maxProperties',
    'minProperties',
    'required',
    'enum',
    'type',
    'items',
    'allOf',
    'properties',
    'additionalProperties',
    'readOnly',
    'xml',
    'externalDocs',
    'example',
}

_VALID_PREFIX = 'x-'


class OrderedLazyDict(LazyDict, OrderedDict):
    pass


class OpenAPIConverter(object):
    """Converter generating OpenAPI specification from Marshmallow schemas and fields

    :param str|OpenAPIVersion openapi_version: The OpenAPI version to use.
        Should be in the form '2.x' or '3.x.x' to comply with the OpenAPI standard.
    """
    def __init__(self, openapi_version):
        self.openapi_version = OpenAPIVersion(openapi_version)
        # Schema references
        self.refs = {}
        # Field mappings
        self.field_mapping = DEFAULT_FIELD_MAPPING

    @staticmethod
    def _observed_name(field, name):
        """Adjust field name to reflect `dump_to` and `load_from` attributes.

        :param Field field: A marshmallow field.
        :param str name: Field name
        :rtype: str
        """
        # XXX: Check before update marshmallow
        # if MARSHMALLOW_VERSION_INFO[0] < 3:
            # use getattr in case we're running against older versions of marshmallow.
        dump_to = getattr(field, 'dump_to', None)
        load_from = getattr(field, 'load_from', None)
        return dump_to or load_from or name
        # return field.data_key or name

    def map_to_openapi_type(self, *args):
        """Decorator to set mapping for custom fields.

        ``*args`` can be:

        - a pair of the form ``(type, format)``
        - a core marshmallow field type (in which case we reuse that type's mapping)
        """
        if len(args) == 1 and args[0] in self.field_mapping:
            openapi_type_field = self.field_mapping[args[0]]
        elif len(args) == 2:
            openapi_type_field = args
        else:
            raise TypeError('Pass core marshmallow field type or (type, fmt) pair')

        def inner(field_type):
            self.field_mapping[field_type] = openapi_type_field
            return field_type

        return inner

    def field2choices(self, field, **kwargs):
        """Return the dictionary of OpenAPI field attributes for valid choices definition

        :param Field field: A marshmallow field.
        :rtype: dict
        """
        attributes = {}

        comparable = [
            validator.comparable for validator in field.validators
            if hasattr(validator, 'comparable')
        ]
        if comparable:
            attributes['enum'] = comparable
        else:
            choices = [
                OrderedSet(validator.choices) for validator in field.validators
                if hasattr(validator, 'choices')
            ]
            if choices:
                attributes['enum'] = list(functools.reduce(operator.and_, choices))

        return attributes

    def field2read_only(self, field, **kwargs):
        """Return the dictionary of OpenAPI field attributes for a dump_only field.

        :param Field field: A marshmallow field.
        :rtype: dict
        """
        attributes = {}
        if field.dump_only:
            attributes['readOnly'] = True
        return attributes

    def field2write_only(self, field, **kwargs):
        """Return the dictionary of OpenAPI field attributes for a load_only field.

        :param Field field: A marshmallow field.
        :rtype: dict
        """
        attributes = {}
        if field.load_only and self.openapi_version.major >= 3:
            attributes['writeOnly'] = True
        return attributes

    def field2nullable(self, field, **kwargs):
        """Return the dictionary of OpenAPI field attributes for a nullable field.

        :param Field field: A marshmallow field.
        :rtype: dict
        """
        attributes = {}
        if field.allow_none:
            attributes['x-nullable' if self.openapi_version.major < 3 else 'nullable'] = True
        return attributes

    def field2range(self, field, **kwargs):
        """Return the dictionary of OpenAPI field attributes for a set of
        :class:`Range <marshmallow.validators.Range>` validators.

        :param Field field: A marshmallow field.
        :rtype: dict
        """
        validators = [
            validator for validator in field.validators
            if (
                hasattr(validator, 'min') and
                hasattr(validator, 'max') and
                not hasattr(validator, 'equal')
            )
        ]

        attributes = {}
        for validator in validators:
            if validator.min is not None:
                if hasattr(attributes, 'minimum'):
                    attributes['minimum'] = max(
                        attributes['minimum'],
                        validator.min,
                    )
                else:
                    attributes['minimum'] = validator.min
            if validator.max is not None:
                if hasattr(attributes, 'maximum'):
                    attributes['maximum'] = min(
                        attributes['maximum'],
                        validator.max,
                    )
                else:
                    attributes['maximum'] = validator.max
        return attributes

    def field2length(self, field, **kwargs):
        """Return the dictionary of OpenAPI field attributes for a set of
        :class:`Length <marshmallow.validators.Length>` validators.

        :param Field field: A marshmallow field.
        :rtype: dict
        """
        attributes = {}

        validators = [
            validator for validator in field.validators
            if (
                hasattr(validator, 'min') and
                hasattr(validator, 'max') and
                hasattr(validator, 'equal')
            )
        ]

        is_array = isinstance(
            field, (
                marshmallow.fields.Nested,
                marshmallow.fields.List,
            ),
        )
        min_attr = 'minItems' if is_array else 'minLength'
        max_attr = 'maxItems' if is_array else 'maxLength'

        for validator in validators:
            if validator.min is not None:
                if hasattr(attributes, min_attr):
                    attributes[min_attr] = max(
                        attributes[min_attr],
                        validator.min,
                    )
                else:
                    attributes[min_attr] = validator.min
            if validator.max is not None:
                if hasattr(attributes, max_attr):
                    attributes[max_attr] = min(
                        attributes[max_attr],
                        validator.max,
                    )
                else:
                    attributes[max_attr] = validator.max

        for validator in validators:
            if validator.equal is not None:
                attributes[min_attr] = validator.equal
                attributes[max_attr] = validator.equal
        return attributes

    def field2property(self, field, use_refs=True, dump=True, name=None):
        """Return the JSON Schema property definition given a marshmallow
        :class:`Field <marshmallow.fields.Field>`.

        Will include field metadata that are valid properties of OpenAPI schema objects
        (e.g. "description", "enum", "example").

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#schemaObject

        :param Field field: A marshmallow field.
        :param bool use_refs: Use JSONSchema ``refs``.
        :param bool dump: Introspect dump logic.
        :param str name: The definition name, if applicable, used to construct the $ref value.
        :rtype: dict, a Property Object
        """

        type_, fmt = self.field_mapping.get(type(field), ('string', None))

        ret = {
            'type': type_,
        }

        if fmt:
            ret['format'] = fmt

        default = field.missing
        if default is not marshmallow.missing:
            if callable(default):
                ret['default'] = default()
            else:
                ret['default'] = default

        for attr_func in (
                self.field2choices,
                self.field2read_only,
                self.field2write_only,
                self.field2nullable,
                self.field2range,
                self.field2length,
        ):
            ret.update(attr_func(field))

        if isinstance(field, marshmallow.fields.Nested):
            del ret['type']
            # marshmallow>=2.7.0 compat
            field.metadata.pop('many', None)

            is_unbound_self_referencing = not getattr(field, 'parent', None) and field.nested == 'self'
            if (use_refs and 'ref' in field.metadata) or is_unbound_self_referencing:
                if 'ref' in field.metadata:
                    ref_name = field.metadata['ref']
                else:
                    if not name:
                        raise ValueError(
                            'Must pass `name` argument for self-referencing Nested fields.',
                        )
                    # We need to use the `name` argument when the field is self-referencing and
                    # unbound (doesn't have `parent` set) because we can't access field.schema
                    ref_path = self.get_ref_path()
                    ref_name = '#/{ref_path}/{name}'.format(ref_path=ref_path, name=name)
                ref_schema = {'$ref': ref_name}
                if field.many:
                    ret['type'] = 'array'
                    ret['items'] = ref_schema
                else:
                    if ret:
                        ret.update({'allOf': [ref_schema]})
                    else:
                        ret.update(ref_schema)
            else:
                schema_dict = self.resolve_schema_dict(field.schema, dump=dump)
                if ret and '$ref' in schema_dict:
                    ret.update({'allOf': [schema_dict]})
                else:
                    ret.update(schema_dict)
        elif isinstance(field, marshmallow.fields.List):
            ret['items'] = self.field2property(
                field.container, use_refs=use_refs, dump=dump,
            )
        elif isinstance(field, marshmallow.fields.Dict):
            if MARSHMALLOW_VERSION_INFO[0] >= 3:
                if field.value_container:
                    ret['additionalProperties'] = self.field2property(
                        field.value_container, use_refs=use_refs, dump=dump,
                    )

        # Dasherize metadata that starts with x_
        metadata = {
            key.replace('_', '-') if key.startswith('x_') else key: value
            for key, value in iteritems(field.metadata)
        }
        for key, value in iteritems(metadata):
            if key in _VALID_PROPERTIES or key.startswith(_VALID_PREFIX):
                ret[key] = value
        # Avoid validation error with "Additional properties not allowed"
        # Property "ref" is not valid in this context
        ret.pop('ref', None)

        return ret


    def schema2parameters(self, schema, **kwargs):
        """Return an array of OpenAPI parameters given a given marshmallow
        :class:`Schema <marshmallow.Schema>`. If `default_in` is "body", then return an array
        of a single parameter; else return an array of a parameter for each included field in
        the :class:`Schema <marshmallow.Schema>`.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#parameterObject
        """
        if hasattr(schema, 'fields'):
            fields = schema.fields
        elif hasattr(schema, '_declared_fields'):
            fields = copy.deepcopy(schema._declared_fields)
        else:
            raise ValueError(
                "{0!r} doesn't have either `fields` or `_declared_fields`".format(schema),
            )
        return self.fields2parameters(fields, schema, **kwargs)


    def fields2parameters(
        self, fields, schema=None, use_refs=True,
        default_in='body', name='body', required=False,
        use_instances=False, description=None, **kwargs
    ):
        """Return an array of OpenAPI parameters given a mapping between field names and
        :class:`Field <marshmallow.Field>` objects. If `default_in` is "body", then return an array
        of a single parameter; else return an array of a parameter for each included field in
        the :class:`Schema <marshmallow.Schema>`.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#parameterObject

        In OpenAPI3, only "query", "header", "path" or "cookie" are allowed for the location
        of parameters. In OpenAPI 3, "requestBody" is used when fields are in the body.

        This function always returns a list, with a parameter
        for each included field in the :class:`Schema <marshmallow.Schema>`.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.1.md#parameterObject
        """
        openapi_default_in = __location_map__.get(default_in, default_in)
        if self.openapi_version.major < 3 and openapi_default_in == 'body':
            if schema is not None:
                prop = self.resolve_schema_dict(schema, dump=False, use_instances=use_instances)
            else:
                prop = self.fields2jsonschema(fields, use_refs=use_refs, dump=False)

            param = {
                'in': openapi_default_in,
                'required': required,
                'name': name,
                'schema': prop,
            }

            if description:
                param['description'] = description

            return [param]

        assert not getattr(schema, 'many', False), \
            "Schemas with many=True are only supported for 'json' location (aka 'in: body')"

        exclude_fields = getattr(getattr(schema, 'Meta', None), 'exclude', [])
        dump_only_fields = getattr(getattr(schema, 'Meta', None), 'dump_only', [])

        parameters = []
        body_param = None
        for field_name, field_obj in iteritems(fields):
            if (field_name in exclude_fields or field_obj.dump_only or field_name in dump_only_fields):
                continue
            param = self.field2parameter(
                field_obj,
                name=self._observed_name(field_obj, field_name),
                use_refs=use_refs,
                default_in=default_in,
            )
            if self.openapi_version.major < 3 and param['in'] == 'body' and body_param is not None:
                body_param['schema']['properties'].update(param['schema']['properties'])
                required_fields = param['schema'].get('required', [])
                if required_fields:
                    body_param['schema'].setdefault('required', []).extend(required_fields)
            else:
                if self.openapi_version.major < 3 and param['in'] == 'body':
                    body_param = param
                parameters.append(param)
        return parameters


    def field2parameter(self, field, name='body', use_refs=True, default_in='body'):
        """Return an OpenAPI parameter as a `dict`, given a marshmallow
        :class:`Field <marshmallow.Field>`.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#parameterObject
        """
        location = field.metadata.get('location', None)
        prop = self.field2property(field, use_refs=use_refs, dump=False)
        return self.property2parameter(
            prop,
            name=name,
            required=field.required,
            multiple=isinstance(field, marshmallow.fields.List),
            location=location,
            default_in=default_in,
        )


    def property2parameter(
        self, prop, name='body', required=False, multiple=False,
        location=None, default_in='body',
    ):
        """Return the Parameter Object definition for a JSON Schema property.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#parameterObject

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
        ret = {
            'in': openapi_location,
            'name': name,
        }

        if openapi_location == 'body':
            ret['required'] = False
            ret['name'] = 'body'
            ret['schema'] = {
                'type': 'object',
                'properties': {name: prop} if name else {},
            }
            if name and required:
                ret['schema']['required'] = [name]
        else:
            ret['required'] = required
            if self.openapi_version.major < 3:
                if multiple:
                    ret['collectionFormat'] = 'multi'
                ret.update(prop)
            else:
                if multiple:
                    ret['explode'] = True
                    ret['style'] = 'form'
                if prop.get('description', None):
                    ret['description'] = prop.pop('description')
                ret['schema'] = prop
        return ret

    def schema2jsonschema(self, schema, use_refs=True, dump=True, name=None):
        if hasattr(schema, 'fields'):
            fields = schema.fields
        elif hasattr(schema, '_declared_fields'):
            fields = copy.deepcopy(schema._declared_fields)
        else:
            raise ValueError(
                "{0!r} doesn't have either `fields` or `_declared_fields`".format(schema),
            )

        return self.fields2jsonschema(
            fields, schema, use_refs=use_refs, dump=dump, name=name,
        )


    def fields2jsonschema(self, fields, schema=None, use_refs=True, dump=True, name=None):
        """Return the JSON Schema Object for a given marshmallow
        :class:`Schema <marshmallow.Schema>`. Schema may optionally provide the ``title`` and
        ``description`` class Meta options.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#schemaObject

        Example: ::

            class UserSchema(Schema):
                _id = fields.Int()
                email = fields.Email(description='email address of the user')
                name = fields.Str()

                class Meta:
                    title = 'User'
                    description = 'A registered user'

            OpenAPI.schema2jsonschema(UserSchema)
            # {
            #     'title': 'User', 'description': 'A registered user',
            #     'properties': {
            #         'name': {'required': False,
            #                 'description': '',
            #                 'type': 'string'},
            #         '_id': {'format': 'int32',
            #                 'required': False,
            #                 'description': '',
            #                 'type': 'integer'},
            #         'email': {'format': 'email',
            #                 'required': False,
            #                 'description': 'email address of the user',
            #                 'type': 'string'}
            #     }
            # }

        :param Schema schema: A marshmallow Schema instance or a class object
        :rtype: dict, a JSON Schema Object
        """
        Meta = getattr(schema, 'Meta', None)
        if getattr(Meta, 'fields', None) or getattr(Meta, 'additional', None):
            declared_fields = set(schema._declared_fields.keys())
            if (
                set(getattr(Meta, 'fields', set())) > declared_fields or
                set(getattr(Meta, 'additional', set())) > declared_fields
            ):
                warnings.warn(
                    'Only explicitly-declared fields will be included in the Schema Object. '
                    'Fields defined in Meta.fields or Meta.additional are ignored.',
                )

        jsonschema = {
            'type': 'object',
            'properties': OrderedLazyDict() if getattr(Meta, 'ordered', None) else LazyDict(),
        }

        exclude = set(getattr(Meta, 'exclude', []))

        for field_name, field_obj in iteritems(fields):
            if field_name in exclude or (field_obj.dump_only and not dump):
                continue

            observed_field_name = self._observed_name(field_obj, field_name)
            prop_func = lambda field_obj=field_obj: self.field2property(  # flake8: noqa
                field_obj, use_refs=use_refs, dump=dump, name=name,
            )
            jsonschema['properties'][observed_field_name] = prop_func

            partial = getattr(schema, 'partial', None)
            if field_obj.required:
                if not partial or (is_collection(partial) and field_name not in partial):
                    jsonschema.setdefault('required', []).append(observed_field_name)

        if 'required' in jsonschema:
            jsonschema['required'].sort()

        if Meta is not None:
            if hasattr(Meta, 'title'):
                jsonschema['title'] = Meta.title
            if hasattr(Meta, 'description'):
                jsonschema['description'] = Meta.description

        if getattr(schema, 'many', False):
            jsonschema = {
                'type': 'array',
                'items': jsonschema,
            }

        return jsonschema


    def get_ref_path(self):
        """Return the path for references based on the openapi version

        :param int openapi_version.major: The major version of the OpenAPI standard
            to use. Supported values are 2 and 3.
        """
        ref_paths = {
            2: 'definitions',
            3: 'components/schemas',
        }
        return ref_paths[self.openapi_version.major]

    def resolve_schema_dict(self, schema, dump=True, use_instances=False):
        if isinstance(schema, dict):
            if schema.get('type') == 'array' and 'items' in schema:
                schema['items'] = self.resolve_schema_dict(
                    schema['items'], use_instances=use_instances,
                )
            if schema.get('type') == 'object' and 'properties' in schema:
                schema['properties'] = {
                    k: self.resolve_schema_dict(v, dump=dump, use_instances=use_instances)
                    for k, v in schema['properties'].items()
                }
            return schema
        if isinstance(schema, marshmallow.Schema) and use_instances:
            schema_cls = schema
        else:
            schema_cls = resolve_schema_cls(schema)

        if schema_cls in self.refs:
            ref_path = self.get_ref_path()
            ref_schema = {'$ref': '#/{0}/{1}'.format(ref_path, self.refs[schema_cls])}
            if getattr(schema, 'many', False):
                return {
                    'type': 'array',
                    'items': ref_schema,
                }
            return ref_schema
        if not isinstance(schema, marshmallow.Schema):
            schema = schema_cls
        return self.schema2jsonschema(schema, dump=dump)
