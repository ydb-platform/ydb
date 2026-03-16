import abc
import contextlib

from marshmallow import Schema, ValidationError
from marshmallow.fields import Field


class PolyFieldBase(Field, metaclass=abc.ABCMeta):
    def __init__(self, many=False, **metadata):
        super().__init__(**metadata)
        self.many = many

    def _deserialize(self, value, attr, parent, partial=None, **kwargs):
        if not self.many:
            value = [value]

        results = []
        for v in value:
            deserializer = None
            try:
                deserializer = self.deserialization_schema_selector(v, parent)
                if isinstance(deserializer, type):
                    deserializer = deserializer()
                if not isinstance(deserializer, (Field, Schema)):
                    raise Exception('Invalid deserializer type')
            except TypeError as te:
                raise ValidationError(str(te)) from te
            except ValidationError:
                raise
            except Exception as err:
                class_type = None
                if deserializer:
                    class_type = str(type(deserializer))

                raise ValidationError(
                    "Unable to use schema. Error: {err}\n"
                    "Ensure there is a deserialization_schema_selector"
                    " and then it returns a field or a schema when the function is passed in "
                    "{value_passed}. This is the class I got. "
                    "Make sure it is a field or a schema: {class_type}".format(
                        err=err,
                        value_passed=v,
                        class_type=class_type
                    )
                ) from err

            # Will raise ValidationError if any problems
            if isinstance(deserializer, Field):
                data = deserializer.deserialize(v, attr, parent)
            else:
                deserializer.context.update(getattr(self, 'context', {}))
                data = deserializer.load(v, partial=partial)

            results.append(data)

        if self.many:
            return results
        else:
            # Will be at least one otherwise value would have been None
            return results[0]

    def _serialize(self, value, key, obj, **kwargs):
        if value is None:
            return None
        try:
            if self.many:
                res = []
                for v in value:
                    schema = self.serialization_schema_selector(v, obj)
                    if isinstance(schema, type):
                        schema = schema()
                    with contextlib.suppress(AttributeError, TypeError):
                        schema.context.update(getattr(self, 'context', {}))
                    serialized = (schema.dump(v)
                                  if hasattr(schema, 'dump')
                                  else schema._serialize(v, None, None))
                    res.append(serialized)
                return res
            else:
                schema = self.serialization_schema_selector(value, obj)
                if isinstance(schema, type):
                    schema = schema()
                with contextlib.suppress(AttributeError, TypeError):
                    schema.context.update(getattr(self, 'context', {}))
                return (schema.dump(value)
                        if hasattr(schema, 'dump')
                        else schema._serialize(value, None, None))
        except Exception as err:
            raise TypeError(
                'Failed to serialize object. Error: {0}\n'
                ' Ensure the serialization_schema_selector exists and '
                ' returns a Schema and that schema'
                ' can serialize this value {1}'.format(err, value))

    @abc.abstractmethod
    def serialization_schema_selector(self, value, obj):
        raise NotImplementedError

    @abc.abstractmethod
    def deserialization_schema_selector(self, value, obj):
        raise NotImplementedError


class PolyField(PolyFieldBase):
    """
    A field that (de)serializes to one of many types. Passed in functions
    are called to disambiguate what schema to use for the (de)serialization
    Intended to assist in working with fields that can contain any subclass
    of a base type
    """
    def __init__(
            self,
            serialization_schema_selector=None,
            deserialization_schema_selector=None,
            many=False,
            **metadata
    ):
        """
        :param serialization_schema_selector: Function that takes in either
        an object representing that object, it's parent object
        and returns the appropriate schema.
        :param deserialization_schema_selector: Function that takes in either
        an a dict representing that object, dict representing it's parent dict
        and returns the appropriate schema

        """
        super().__init__(many=many, **metadata)
        self._serialization_schema_selector_arg = serialization_schema_selector
        self._deserialization_schema_selector_arg = deserialization_schema_selector

    def serialization_schema_selector(self, value, obj):
        return self._serialization_schema_selector_arg(value, obj)

    def deserialization_schema_selector(self, value, obj):
        return self._deserialization_schema_selector_arg(value, obj)
