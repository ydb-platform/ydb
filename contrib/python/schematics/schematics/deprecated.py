
import warnings
import functools

from collections import OrderedDict

from .compat import iteritems
from .types.serializable import Serializable
from . import transforms


class SchematicsDeprecationWarning(DeprecationWarning):
    pass


def deprecated(func):
    # Schematics seems unmaintained, no point in warning about deprecations anyway.
    return func


class SchemaCompatibilityMixin(object):
    """Compatibility layer for previous deprecated Schematics Model API."""

    @property
    @deprecated
    def __name__(self):
        return self.name

    @property
    @deprecated
    def _options(self):
        return self.options

    @property
    @deprecated
    def _validator_functions(self):
        return self.validators

    @property
    @deprecated
    def _fields(self):
        return self.fields

    @property
    @deprecated
    def _valid_input_keys(self):
        return self.valid_input_keys

    @property
    @deprecated
    def _serializables(self):
        return OrderedDict((k, t) for k, t in iteritems(self.fields) if isinstance(t, Serializable))


class class_property(property):
    def __get__(self, instance, type=None):
        if instance is None:
            return super(class_property, self).__get__(type, type)
        return super(class_property, self).__get__(instance, type)


class ModelCompatibilityMixin(object):
    """Compatibility layer for previous deprecated Schematics Model API."""

    @class_property
    @deprecated
    def _valid_input_keys(cls):
        return cls._schema.valid_input_keys

    @class_property
    @deprecated
    def _options(cls):
        return cls._schema.options

    @class_property
    @deprecated
    def fields(cls):
        return cls._schema.fields

    @class_property
    @deprecated
    def _fields(cls):
        return cls._schema.fields

    @class_property
    @deprecated
    def _field_list(cls):
        return list(iteritems(cls._schema.fields))

    @class_property
    @deprecated
    def _serializables(cls):
        return cls._schema._serializables

    @class_property
    @deprecated
    def _validator_functions(cls):
        return cls._schema.validators

    @classmethod
    @deprecated
    def convert(cls, raw_data, context=None, **kw):
        return transforms.convert(cls._schema, raw_data, oo=True,
            context=context, **kw)


class BaseErrorV1Mixin(object):

    @property
    @deprecated
    def messages(self):
        """ an alias for errors, provided for compatibility with V1. """
        return self.errors


def patch_models():
    global models_Model
    from . import schema
    from . import models
    models_Model = models.Model
    class Model(ModelCompatibilityMixin, models.Model):
        __doc__ = models.Model.__doc__
    models.Model = Model
    models.ModelOptions = schema.SchemaOptions  # deprecated alias


def patch_schema():
    global schema_Schema
    from . import schema
    schema_Schema = schema.Schema
    class Schema(SchemaCompatibilityMixin, schema.Schema):
        __doc__ = schema.Schema.__doc__
    schema.Schema = Schema


def patch_exceptions():
    from . import exceptions
    exceptions.BaseError.messages = BaseErrorV1Mixin.messages
    exceptions.ModelConversionError = exceptions.DataError  # v1
    exceptions.ModelValidationError = exceptions.DataError  # v1
    exceptions.StopValidation = exceptions.StopValidationError  # v1


def patch_all():
    patch_schema()
    patch_models()
    patch_exceptions()
