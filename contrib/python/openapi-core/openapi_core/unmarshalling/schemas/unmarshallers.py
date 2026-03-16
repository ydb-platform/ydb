from __future__ import division
from functools import partial
import logging

from isodate.isodatetime import parse_datetime

from openapi_schema_validator._types import (
    is_array, is_bool, is_integer,
    is_object, is_number, is_string,
)
from openapi_schema_validator._format import oas30_format_checker
from six import text_type, binary_type
from six import iteritems

from openapi_core.extensions.models.factories import ModelFactory
from openapi_core.schema.schemas import (
    get_all_properties, get_all_properties_names
)
from openapi_core.types import NoValue
from openapi_core.unmarshalling.schemas.enums import UnmarshalContext
from openapi_core.unmarshalling.schemas.exceptions import (
    UnmarshalError, ValidateError, InvalidSchemaValue,
    InvalidSchemaFormatValue,
)
from openapi_core.unmarshalling.schemas.formatters import Formatter
from openapi_core.unmarshalling.schemas.util import (
    forcebool, format_date, format_byte, format_uuid,
    format_number,
)

log = logging.getLogger(__name__)


class PrimitiveTypeUnmarshaller(object):

    FORMATTERS = {}

    def __init__(self, formatter, validator, schema):
        self.formatter = formatter
        self.validator = validator
        self.schema = schema

    def __call__(self, value=NoValue):
        if value is NoValue:
            value = self.schema.getkey('default')
        if value is None:
            return

        self.validate(value)

        return self.unmarshal(value)

    def _formatter_validate(self, value):
        result = self.formatter.validate(value)
        if not result:
            schema_type = self.schema.getkey('type', 'any')
            raise InvalidSchemaValue(value, schema_type)

    def validate(self, value):
        errors_iter = self.validator.iter_errors(value)
        errors = tuple(errors_iter)
        if errors:
            schema_type = self.schema.getkey('type', 'any')
            raise InvalidSchemaValue(
                value, schema_type, schema_errors=errors)

    def unmarshal(self, value):
        try:
            return self.formatter.unmarshal(value)
        except ValueError as exc:
            schema_format = self.schema.getkey('format')
            raise InvalidSchemaFormatValue(
                value, schema_format, exc)


class StringUnmarshaller(PrimitiveTypeUnmarshaller):

    FORMATTERS = {
        None: Formatter.from_callables(
            partial(is_string, None), text_type),
        'password': Formatter.from_callables(
            partial(oas30_format_checker.check, format='password'), text_type),
        'date': Formatter.from_callables(
            partial(oas30_format_checker.check, format='date'), format_date),
        'date-time': Formatter.from_callables(
            partial(oas30_format_checker.check, format='date-time'),
            parse_datetime),
        'binary': Formatter.from_callables(
            partial(oas30_format_checker.check, format='binary'), binary_type),
        'uuid': Formatter.from_callables(
            partial(oas30_format_checker.check, format='uuid'), format_uuid),
        'byte': Formatter.from_callables(
            partial(oas30_format_checker.check, format='byte'), format_byte),
    }


class IntegerUnmarshaller(PrimitiveTypeUnmarshaller):

    FORMATTERS = {
        None: Formatter.from_callables(
            partial(is_integer, None), int),
        'int32': Formatter.from_callables(
            partial(oas30_format_checker.check, format='int32'), int),
        'int64': Formatter.from_callables(
            partial(oas30_format_checker.check, format='int64'), int),
    }


class NumberUnmarshaller(PrimitiveTypeUnmarshaller):

    FORMATTERS = {
        None: Formatter.from_callables(
            partial(is_number, None), format_number),
        'float': Formatter.from_callables(
            partial(oas30_format_checker.check, format='float'), float),
        'double': Formatter.from_callables(
            partial(oas30_format_checker.check, format='double'), float),
    }


class BooleanUnmarshaller(PrimitiveTypeUnmarshaller):

    FORMATTERS = {
        None: Formatter.from_callables(
            partial(is_bool, None), forcebool),
    }


class ComplexUnmarshaller(PrimitiveTypeUnmarshaller):

    def __init__(
            self, formatter, validator, schema, unmarshallers_factory,
            context=None):
        super(ComplexUnmarshaller, self).__init__(formatter, validator, schema)
        self.unmarshallers_factory = unmarshallers_factory
        self.context = context


class ArrayUnmarshaller(ComplexUnmarshaller):

    FORMATTERS = {
        None: Formatter.from_callables(
            partial(is_array, None), list),
    }

    @property
    def items_unmarshaller(self):
        return self.unmarshallers_factory.create(self.schema / 'items')

    def __call__(self, value=NoValue):
        value = super(ArrayUnmarshaller, self).__call__(value)
        if value is None and self.schema.getkey('nullable', False):
            return None
        return list(map(self.items_unmarshaller, value))


class ObjectUnmarshaller(ComplexUnmarshaller):

    FORMATTERS = {
        None: Formatter.from_callables(
            partial(is_object, None), dict),
    }

    @property
    def model_factory(self):
        return ModelFactory()

    def unmarshal(self, value):
        try:
            value = self.formatter.unmarshal(value)
        except ValueError as exc:
            raise InvalidSchemaFormatValue(
                value, self.schema.format, exc)
        else:
            return self._unmarshal_object(value)

    def _unmarshal_object(self, value=NoValue):
        if 'oneOf' in self.schema:
            properties = None
            for one_of_schema in self.schema / 'oneOf':
                try:
                    unmarshalled = self._unmarshal_properties(
                        value, one_of_schema)
                except (UnmarshalError, ValueError):
                    pass
                else:
                    if properties is not None:
                        log.warning("multiple valid oneOf schemas found")
                        continue
                    properties = unmarshalled

            if properties is None:
                log.warning("valid oneOf schema not found")

        else:
            properties = self._unmarshal_properties(value)

        if 'x-model' in self.schema:
            name = self.schema['x-model']
            return self.model_factory.create(properties, name=name)

        return properties

    def _unmarshal_properties(self, value=NoValue, one_of_schema=None):
        all_props = get_all_properties(self.schema)
        all_props_names = get_all_properties_names(self.schema)

        if one_of_schema is not None:
            all_props.update(get_all_properties(one_of_schema))
            all_props_names |= get_all_properties_names(one_of_schema)

        value_props_names = value.keys()
        extra_props = set(value_props_names) - set(all_props_names)

        properties = {}
        additional_properties = self.schema.getkey(
            'additionalProperties', True)
        if isinstance(additional_properties, dict):
            additional_prop_schema = self.schema / 'additionalProperties'
            for prop_name in extra_props:
                prop_value = value[prop_name]
                properties[prop_name] = self.unmarshallers_factory.create(
                    additional_prop_schema)(prop_value)
        elif additional_properties is True:
            for prop_name in extra_props:
                prop_value = value[prop_name]
                properties[prop_name] = prop_value

        for prop_name, prop in iteritems(all_props):
            read_only = prop.getkey('readOnly', False)
            if self.context == UnmarshalContext.REQUEST and read_only:
                continue
            write_only = prop.getkey('writeOnly', False)
            if self.context == UnmarshalContext.RESPONSE and write_only:
                continue
            try:
                prop_value = value[prop_name]
            except KeyError:
                if 'default' not in prop:
                    continue
                prop_value = prop['default']

            properties[prop_name] = self.unmarshallers_factory.create(
                prop)(prop_value)

        return properties


class AnyUnmarshaller(ComplexUnmarshaller):

    FORMATTERS = {
        None: Formatter(),
    }

    SCHEMA_TYPES_ORDER = [
        'object', 'array', 'boolean',
        'integer', 'number', 'string',
    ]

    def unmarshal(self, value=NoValue):
        one_of_schema = self._get_one_of_schema(value)
        if one_of_schema:
            return self.unmarshallers_factory.create(one_of_schema)(value)

        all_of_schema = self._get_all_of_schema(value)
        if all_of_schema:
            return self.unmarshallers_factory.create(all_of_schema)(value)

        for schema_type in self.SCHEMA_TYPES_ORDER:
            unmarshaller = self.unmarshallers_factory.create(
                self.schema, type_override=schema_type)
            # validate with validator of formatter (usualy type validator)
            try:
                unmarshaller._formatter_validate(value)
            except ValidateError:
                continue
            else:
                return unmarshaller(value)

        log.warning("failed to unmarshal any type")
        return value

    def _get_one_of_schema(self, value):
        if 'oneOf' not in self.schema:
            return

        one_of_schemas = self.schema / 'oneOf'
        for subschema in one_of_schemas:
            unmarshaller = self.unmarshallers_factory.create(subschema)
            try:
                unmarshaller.validate(value)
            except ValidateError:
                continue
            else:
                return subschema

    def _get_all_of_schema(self, value):
        if 'allOf' not in self.schema:
            return

        all_of_schemas = self.schema / 'allOf'
        for subschema in all_of_schemas:
            if 'type' not in subschema:
                continue
            unmarshaller = self.unmarshallers_factory.create(subschema)
            try:
                unmarshaller.validate(value)
            except ValidateError:
                continue
            else:
                return subschema
