# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

from copy import deepcopy
import inspect
from collections import OrderedDict
from types import FunctionType

from .common import *
from .compat import str_compat, repr_compat, _dict
from .datastructures import Context, ChainMap, MappingProxyType
from .exceptions import *
from .iteration import atoms
from .transforms import (
    export_loop, convert,
    to_native, to_primitive,
)
from .validate import validate, prepare_validator
from .types import BaseType
from .types.serializable import Serializable
from .undefined import Undefined
from .util import get_ident
from . import schema

__all__ = []


class FieldDescriptor(object):
    """
    ``FieldDescriptor`` instances serve as field accessors on models.
    """

    def __init__(self, name):
        """
        :param name:
            The field's name
        """
        self.name = name

    def __get__(self, instance, cls):
        """
        For a model instance, returns the field's current value.
        For a model class, returns the field's type object.
        """
        if instance is None:
            return cls._fields[self.name]
        else:
            value = instance._data.get(self.name, Undefined)
            if value is Undefined:
                raise UndefinedValueError(instance, self.name)
            else:
                return value

    def __set__(self, instance, value):
        """
        Sets the field's value.
        """
        field = instance._fields[self.name]
        value = field.pre_setattr(value)
        instance._data.converted[self.name] = value

    def __delete__(self, instance):
        """
        Deletes the field's value.
        """
        del instance._data[self.name]


class ModelMeta(type):
    """
    Metaclass for Models.
    """

    def __new__(mcs, name, bases, attrs):
        """
        This metaclass parses the declarative Model into a corresponding Schema,
        then adding it as the `_schema` attribute to the host class.
        """

        # Structures used to accumulate meta info
        fields = OrderedDict()
        validator_functions = {}  # Model level
        options_members = {}

        # Accumulate metas info from parent classes
        for base in reversed(bases):
            if hasattr(base, '_schema'):
                fields.update(deepcopy(base._schema.fields))
                options_members.update(dict(base._schema.options))
                validator_functions.update(base._schema.validators)

        # Parse this class's attributes into schema structures
        for key, value in iteritems(attrs):
            if key.startswith('validate_') and isinstance(value, (FunctionType, classmethod)):
                validator_functions[key[9:]] = prepare_validator(value, 4)
            if isinstance(value, BaseType):
                fields[key] = value
            elif isinstance(value, Serializable):
                fields[key] = value

        # Convert declared fields into descriptors for new class
        fields = OrderedDict(sorted(
            (kv for kv in fields.items()),
            key=lambda i: i[1]._position_hint,
        ))
        for key, field in iteritems(fields):
            if isinstance(field, BaseType):
                attrs[key] = FieldDescriptor(key)
            elif isinstance(field, Serializable):
                attrs[key] = field

        klass = type.__new__(mcs, name, bases, attrs)
        klass = repr_compat(str_compat(klass))

        # Parse schema options
        options = mcs._read_options(name, bases, attrs, options_members)

        # Parse meta data into new schema
        klass._schema = schema.Schema(name, model=klass, options=options,
            validators=validator_functions, *(schema.Field(k, t) for k, t in iteritems(fields)))

        return klass

    @classmethod
    def _read_options(mcs, name, bases, attrs, options_members):
        """
        Parses model `Options` class into a `SchemaOptions` instance.
        """
        options_class = attrs.get('__optionsclass__', schema.SchemaOptions)
        if 'Options' in attrs:
            for key, value in inspect.getmembers(attrs['Options']):
                if key.startswith("__"):
                    continue
                elif key.startswith("_"):
                    extras = options_members.get("extras", {}).copy()
                    extras.update({key: value})
                    options_members["extras"] = extras
                elif key == "roles":
                    roles = options_members.get("roles", {}).copy()
                    roles.update(value)
                    options_members[key] = roles
                else:
                    options_members[key] = value
        return options_class(**options_members)


class ModelDict(ChainMap):

    __slots__ = ['_unsafe', '_converted', '__valid', '_valid']

    def __init__(self, unsafe=None, converted=None, valid=None):
        self._unsafe = unsafe if unsafe is not None else {}
        self._converted = converted if converted is not None else {}
        self.__valid = valid if valid is not None else {}
        self._valid = MappingProxyType(self.__valid)
        super(ModelDict, self).__init__(self._unsafe, self._converted, self._valid)

    @property
    def unsafe(self):
        return self._unsafe

    @unsafe.setter
    def unsafe(self, value):
        self._unsafe = value
        self.maps[0] = self._unsafe

    @property
    def converted(self):
        return self._converted

    @converted.setter
    def converted(self, value):
        self._converted = value
        self.maps[1] = self._converted

    @property
    def valid(self):
        return self._valid

    @valid.setter
    def valid(self, value):
        self._valid = MappingProxyType(value)
        self.maps[2] = self._valid

    def __delitem__(self, key):
        did_delete = False
        for data in [self.__valid, self._converted, self._unsafe]:
            try:
                del data[key]
                did_delete = True
            except KeyError:
                pass
        if not did_delete:
            raise KeyError(key)

    def __repr__(self):
        return repr(dict(self))


@metaclass(ModelMeta)
class Model(object):

    """
    Enclosure for fields and validation. Same pattern deployed by Django
    models, SQLAlchemy declarative extension and other developer friendly
    libraries.

    :param Mapping raw_data:
        The data to be imported into the model instance.
    :param Mapping deserialize_mapping:
        Can be used to provide alternative input names for fields. Values may be
        strings or lists of strings, keyed by the actual field name.
    :param bool partial:
        Allow partial data to validate. Essentially drops the ``required=True``
        settings from field definitions. Default: True
    :param bool strict:
        Complain about unrecognized keys. Default: True
    """

    def __init__(self, raw_data=None, trusted_data=None, deserialize_mapping=None,
                 init=True, partial=True, strict=True, validate=False, app_data=None,
                 lazy=False, **kwargs):
        kwargs.setdefault('init_values', init)
        kwargs.setdefault('apply_defaults', init)

        if lazy:
            self._data = ModelDict(unsafe=raw_data, valid=trusted_data)
            return

        self._data = ModelDict(valid=trusted_data)
        data = self._convert(raw_data,
            trusted_data=trusted_data, mapping=deserialize_mapping,
            partial=partial, strict=strict, validate=validate, new=True,
            app_data=app_data, **kwargs)
        self._data.converted = data
        if validate:
            self.validate(partial=partial, app_data=app_data, **kwargs)

    def validate(self, partial=False, convert=True, app_data=None, **kwargs):
        """
        Validates the state of the model. If the data is invalid, raises a ``DataError``
        with error messages.

        :param bool partial:
            Allow partial data to validate. Essentially drops the ``required=True``
            settings from field definitions. Default: False
        :param convert:
            Controls whether to perform import conversion before validating.
            Can be turned off to skip an unnecessary conversion step if all values
            are known to have the right datatypes (e.g., when validating immediately
            after the initial import). Default: True
        """
        if not self._data.converted and partial:
            return  # no new input data to validate
        try:
            data = self._convert(validate=True,
                partial=partial, convert=convert, app_data=app_data, **kwargs)
            self._data.valid = data
        except DataError as e:
            valid = dict(self._data.valid)
            valid.update(e.partial_data)
            self._data.valid = valid
            raise
        finally:
            self._data.converted = {}

    def import_data(self, raw_data, recursive=False, **kwargs):
        """
        Converts and imports the raw data into an existing model instance.

        :param raw_data:
            The data to be imported.
        """
        data = self._convert(raw_data, trusted_data=_dict(self), recursive=recursive, **kwargs)
        self._data.converted.update(data)
        if kwargs.get('validate'):
            self.validate(convert=False)
        return self

    def _convert(self, raw_data=None, context=None, **kwargs):
        """
        Converts the instance raw data into richer Python constructs according
        to the fields on the model, validating data if requested.

        :param raw_data:
            New data to be imported and converted
        """
        raw_data = _dict(raw_data) if raw_data else self._data.converted
        kwargs['trusted_data'] = kwargs.get('trusted_data') or {}
        kwargs['convert'] = getattr(context, 'convert', kwargs.get('convert', True))
        if self._data.unsafe:
            self._data.unsafe.update(raw_data)
            raw_data = self._data.unsafe
            self._data.unsafe = {}
            kwargs['convert'] = True
        should_validate = getattr(context, 'validate', kwargs.get('validate', False))
        func = validate if should_validate else convert
        return func(self._schema, self, raw_data=raw_data, oo=True, context=context, **kwargs)

    def export(self, field_converter=None, role=None, app_data=None, **kwargs):
        return export_loop(self._schema, self, field_converter=field_converter,
                           role=role, app_data=app_data, **kwargs)

    def to_native(self, role=None, app_data=None, **kwargs):
        return to_native(self._schema, self, role=role, app_data=app_data, **kwargs)

    def to_primitive(self, role=None, app_data=None, **kwargs):
        return to_primitive(self._schema, self, role=role, app_data=app_data, **kwargs)

    def serialize(self, *args, **kwargs):
        raw_data = self._data.converted
        try:
            self.validate(apply_defaults=True)
        except DataError:
            pass
        data = self.to_primitive(*args, **kwargs)
        self._data.converted = raw_data
        return data

    def atoms(self):
        """
        Iterator for the atomic components of a model definition and relevant
        data that creates a 3-tuple of the field's name, its type instance and
        its value.
        """
        return atoms(self._schema, self)

    def __iter__(self):
        return (k for k in self._schema.fields if k in self._data
            and getattr(self._schema.fields[k], 'fset', None) is None)

    def keys(self):
        return list(iter(self))

    def items(self):
        return [(k, self._data[k]) for k in self]

    def values(self):
        return [self._data[k] for k in self]

    def get(self, key, default=None):
        return getattr(self, key, default)

    @classmethod
    def _append_field(cls, field_name, field_type):
        """
        Add a new field to this class.

        :type field_name: str
        :param field_name:
            The name of the field to add.
        :type field_type: BaseType
        :param field_type:
            The type to use for the field.
        """
        cls._schema.append_field(schema.Field(field_name, field_type))
        setattr(cls, field_name, FieldDescriptor(field_name))

    @classmethod
    def get_mock_object(cls, context=None, overrides={}):
        """Get a mock object.

        :param dict context:
        :param dict overrides: overrides for the model
        """
        context = Context._make(context)
        context._setdefault('memo', set())
        context.memo.add(cls)
        values = {}
        for name, field in cls.fields.items():
            if name in overrides:
                continue
            if getattr(field, 'model_class', None) in context.memo:
                continue
            try:
                values[name] = field.mock(context)
            except MockCreationError as exc:
                raise MockCreationError('%s: %s' % (name, exc.message))
        values.update(overrides)
        return cls(values)

    def __getitem__(self, name):
        if name in self._schema.fields:
            return getattr(self, name)
        else:
            raise UnknownFieldError(self, name)

    def __setitem__(self, name, value):
        if name in self._schema.fields:
            return setattr(self, name, value)
        else:
            raise UnknownFieldError(self, name)

    def __delitem__(self, name):
        if name in self._schema.fields:
            return delattr(self, name)
        else:
            raise UnknownFieldError(self, name)

    def __contains__(self, name):
        return (name in self._data and getattr(self, name, Undefined) is not Undefined) \
            or name in self._serializables

    def __len__(self):
        return len(self._data)

    def __eq__(self, other, memo=set()):
        if self is other:
            return True
        if type(self) is not type(other):
            return NotImplemented
        key = (id(self), id(other), get_ident())
        if key in memo:
            return True
        else:
            memo.add(key)
        try:
            for k in self:
                if self.get(k) != other.get(k):
                    return False
            return True
        finally:
            memo.remove(key)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        model = self.__class__.__name__
        info = self._repr_info()
        if info:
            return '<%s: %s>' % (model, info)
        else:
            return '<%s instance>' % model

    def _repr_info(self):
        """
        Subclasses may implement this method to augment the ``__repr__()`` output for the instance::

            class Person(Model):
                ...
                def _repr_info(self):
                    return self.name

            >>> Person({'name': 'Mr. Pink'})
            <Person: Mr. Pink>
        """
        return None
