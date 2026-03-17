# -*- coding: utf-8 -*-

from __future__ import unicode_literals, absolute_import

import itertools

from ..common import *
from ..exceptions import *
from ..transforms import (
    export_loop,
    get_import_context, get_export_context,
    to_native_converter, to_primitive_converter)
from ..translator import _
from ..util import get_all_subclasses, import_string
from .base import BaseType, get_value_in

try:
    import typing
except ImportError:
    pass
else:
    T = typing.TypeVar("T")

try:
    from collections.abc import Iterable, Sequence, Mapping  # PY3
except ImportError:
    from collections import Iterable, Sequence, Mapping  # PY2

__all__ = ['CompoundType', 'MultiType', 'ModelType', 'ListType', 'DictType',
    'PolyModelType']


class CompoundType(BaseType):

    def __init__(self, **kwargs):
        super(CompoundType, self).__init__(**kwargs)
        self.is_compound = True
        try:
            self.field.parent_field = self
        except AttributeError:
            pass

    def _setup(self, field_name, owner_model):
        # Recursively set up inner fields.
        if hasattr(self, 'field'):
            self.field._setup(None, owner_model)
        super(CompoundType, self)._setup(field_name, owner_model)

    def convert(self, value, context=None):
        context = context or get_import_context()
        return self._convert(value, context)

    def _convert(self, value, context):
        raise NotImplementedError

    def export(self, value, format, context=None):
        context = context or get_export_context()
        return self._export(value, format, context)

    def _export(self, value, format, context):
        raise NotImplementedError

    def to_native(self, value, context=None):
        context = context or get_export_context(to_native_converter)
        return to_native_converter(self, value, context)

    def to_primitive(self, value, context=None):
        context = context or get_export_context(to_primitive_converter)
        return to_primitive_converter(self, value, context)

    def _init_field(self, field, options):
        """
        Instantiate the inner field that represents each element within this compound type.
        In case the inner field is itself a compound type, its inner field can be provided
        as the ``nested_field`` keyword argument.
        """
        if not isinstance(field, BaseType):
            nested_field = options.pop('nested_field', None) or options.pop('compound_field', None)
            if nested_field:
                field = field(field=nested_field, **options)
            else:
                field = field(**options)
        return field

MultiType = CompoundType


class ModelType(CompoundType):
    """A field that can hold an instance of the specified model."""

    primitive_type = dict

    @property
    def native_type(self):
        return self.model_class

    @property
    def fields(self):
        return self.model_class.fields

    @property
    def model_class(self):
        if self._model_class:
            return self._model_class

        model_class = import_string(self.model_name)
        self._model_class = model_class
        return model_class

    def __init__(self,
                 model_spec,  # type: typing.Type[T]
                 **kwargs):
        # type: (...) -> T

        if isinstance(model_spec, ModelMeta):
            self._model_class = model_spec
            self.model_name = self.model_class.__name__
        elif isinstance(model_spec, string_type):
            self._model_class = None
            self.model_name = model_spec
        else:
            raise TypeError("ModelType: Expected a model, got an argument "
                            "of the type '{}'.".format(model_spec.__class__.__name__))

        super(ModelType, self).__init__(**kwargs)

    def _repr_info(self):
        return self.model_class.__name__

    def _mock(self, context=None):
        return self.model_class.get_mock_object(context)

    def _setup(self, field_name, owner_model):
        # Resolve possible name-based model reference.
        if not self._model_class:
            if self.model_name == owner_model.__name__:
                self._model_class = owner_model
            else:
                pass  # Intentionally left blank, it will be setup later.
        super(ModelType, self)._setup(field_name, owner_model)

    def pre_setattr(self, value):
        if value is not None \
          and not isinstance(value, Model):
            if not isinstance(value, dict):
                raise ConversionError(_('Model conversion requires a model or dict'))
            value = self.model_class(value)
        return value

    def _convert(self, value, context):
        field_model_class = self.model_class
        if isinstance(value, field_model_class):
            model_class = type(value)
        elif isinstance(value, dict):
            model_class = field_model_class
        else:
            raise ConversionError(
                _("Input must be a mapping or '%s' instance") % field_model_class.__name__)
        if context.convert and context.oo:
            return model_class(value, context=context)
        else:
            return model_class.convert(value, context=context)

    def _export(self, value, format, context):
        if isinstance(value, Model):
            model_class = type(value)
        else:
            model_class = self.model_class
        return export_loop(model_class, value, context=context)


class ListType(CompoundType):
    """A field for storing a list of items, all of which must conform to the type
    specified by the ``field`` parameter.

    Use it like this::

        ...
        categories = ListType(StringType)
    """

    primitive_type = list
    native_type = list

    def __init__(self,
                 field,  # type: T
                 min_size=None, max_size=None, **kwargs):
        # type: (...) -> typing.List[T]

        self.field = self._init_field(field, kwargs)
        self.min_size = min_size
        self.max_size = max_size

        validators = [self.check_length] + kwargs.pop("validators", [])

        super(ListType, self).__init__(validators=validators, **kwargs)

    @property
    def model_class(self):
        return self.field.model_class

    def _repr_info(self):
        return self.field.__class__.__name__

    def _mock(self, context=None):
        random_length = get_value_in(self.min_size, self.max_size)

        return [self.field._mock(context) for dummy in range(random_length)]

    def _coerce(self, value):
        if isinstance(value, list):
            return value
        elif isinstance(value, (string_type, Mapping)): # unacceptable iterables
            pass
        elif isinstance(value, Sequence):
            return value
        elif isinstance(value, Iterable):
            return value
        raise ConversionError(_('Could not interpret the value as a list'))

    def _convert(self, value, context):
        value = self._coerce(value)
        data = []
        errors = {}
        for index, item in enumerate(value):
            try:
                data.append(context.field_converter(self.field, item, context))
            except BaseError as exc:
                errors[index] = exc
        if errors:
            raise CompoundError(errors)
        return data

    def check_length(self, value, context):
        list_length = len(value) if value else 0

        if self.min_size is not None and list_length < self.min_size:
            message = ({
                True: _('Please provide at least %d item.'),
                False: _('Please provide at least %d items.'),
            }[self.min_size == 1]) % self.min_size
            raise ValidationError(message)

        if self.max_size is not None and list_length > self.max_size:
            message = ({
                True: _('Please provide no more than %d item.'),
                False: _('Please provide no more than %d items.'),
            }[self.max_size == 1]) % self.max_size
            raise ValidationError(message)

    def _export(self, list_instance, format, context):
        """Loops over each item in the model and applies either the field
        transform or the multitype transform.  Essentially functions the same
        as `transforms.export_loop`.
        """
        data = []
        _export_level = self.field.get_export_level(context)
        if _export_level == DROP:
            return data
        for value in list_instance:
            shaped = self.field.export(value, format, context)
            if shaped is None:
                if _export_level <= NOT_NONE:
                    continue
            elif self.field.is_compound and len(shaped) == 0:
                if _export_level <= NONEMPTY:
                    continue
            data.append(shaped)
        return data


class DictType(CompoundType):
    """A field for storing a mapping of items, the values of which must conform to the type
    specified by the ``field`` parameter.

    Use it like this::

        ...
        categories = DictType(StringType)

    """

    primitive_type = dict
    native_type = dict

    def __init__(self, field, coerce_key=None, **kwargs):
        # type: (...) -> typing.Dict[str, T]

        self.field = self._init_field(field, kwargs)
        self.coerce_key = coerce_key or str
        super(DictType, self).__init__(**kwargs)

    @property
    def model_class(self):
        return self.field.model_class

    def _repr_info(self):
        return self.field.__class__.__name__

    def _convert(self, value, context, safe=False):
        if not isinstance(value, Mapping):
            raise ConversionError(_('Only mappings may be used in a DictType'))

        data = {}
        errors = {}
        for k, v in iteritems(value):
            try:
                data[self.coerce_key(k)] = context.field_converter(self.field, v, context)
            except BaseError as exc:
                errors[k] = exc
        if errors:
            raise CompoundError(errors)
        return data

    def _export(self, dict_instance, format, context):
        """Loops over each item in the model and applies either the field
        transform or the multitype transform.  Essentially functions the same
        as `transforms.export_loop`.
        """
        data = {}
        _export_level = self.field.get_export_level(context)
        if _export_level == DROP:
            return data
        for key, value in iteritems(dict_instance):
            shaped = self.field.export(value, format, context)
            if shaped is None:
                if _export_level <= NOT_NONE:
                    continue
            elif self.field.is_compound and len(shaped) == 0:
                if _export_level <= NONEMPTY:
                    continue
            data[key] = shaped
        return data


class PolyModelType(CompoundType):
    """A field that accepts an instance of any of the specified models."""

    primitive_type = dict
    native_type = None  # cannot be determined from a PolyModelType instance

    def __init__(self, model_spec, **kwargs):

        if isinstance(model_spec, (ModelMeta, string_type)):
            self.model_classes = (model_spec,)
            allow_subclasses = True
        elif isinstance(model_spec, Iterable):
            self.model_classes = tuple(model_spec)
            allow_subclasses = False
        else:
            raise Exception("The first argument to PolyModelType.__init__() "
                            "must be a model or an iterable.")

        self.claim_function = kwargs.pop("claim_function", None)
        self.allow_subclasses = kwargs.pop("allow_subclasses", allow_subclasses)

        CompoundType.__init__(self, **kwargs)

    def _setup(self, field_name, owner_model):
        # Resolve possible name-based model references.
        resolved_classes = []
        for m in self.model_classes:
            if isinstance(m, string_type):
                if m == owner_model.__name__:
                    resolved_classes.append(owner_model)
                else:
                    raise Exception("PolyModelType: Unable to resolve model '{}'.".format(m))
            else:
                resolved_classes.append(m)
        self.model_classes = tuple(resolved_classes)
        super(PolyModelType, self)._setup(field_name, owner_model)

    def is_allowed_model(self, model_instance):
        if self.allow_subclasses:
            if isinstance(model_instance, self.model_classes):
                return True
        else:
            if model_instance.__class__ in self.model_classes:
                return True
        return False

    def _convert(self, value, context):

        if value is None:
            return None

        if not context.validate:
            if self.is_allowed_model(value):
                return value
            if not isinstance(value, dict):
                if len(self.model_classes) > 1:
                    instanceof_msg = 'one of: {}'.format(', '.join(
                        cls.__name__ for cls in self.model_classes))
                else:
                    instanceof_msg = self.model_classes[0].__name__
                raise ConversionError(_('Please use a mapping for this field or '
                                        'an instance of {}').format(instanceof_msg))

        model_class = self.find_model(value)
        return model_class(value, context=context)

    def find_model(self, data):
        """Finds the intended type by consulting potential classes or `claim_function`."""

        if self.claim_function:
            kls = self.claim_function(self, data)
            if not kls:
                raise Exception("Input for polymorphic field did not match any model")
            return kls

        fallback = None
        matching_classes = []
        for kls in self._get_candidates():
            try:
                # If a model defines a _claim_polymorphic method, use
                # it to see if the model matches the data.
                kls_claim = kls._claim_polymorphic
            except AttributeError:
                # The first model that doesn't define the hook can be
                # used as a default if there's no match.
                if not fallback:
                    fallback = kls
            else:
                if kls_claim(data):
                    matching_classes.append(kls)

        if not matching_classes and fallback:
            return fallback

        elif len(matching_classes) != 1:
            raise Exception("Got ambiguous input for polymorphic field")

        return matching_classes[0]

    def _export(self, model_instance, format, context):

        model_class = model_instance.__class__
        if not self.is_allowed_model(model_instance):
            raise Exception("Cannot export: {} is not an allowed type".format(model_class))

        return model_instance.export(context=context)

    def _get_candidates(self):
        candidates = self.model_classes

        if self.allow_subclasses:
            candidates = itertools.chain.from_iterable(
                ([m] + get_all_subclasses(m) for m in candidates)
            )

        return candidates


if PY2:
    # Python 2 names cannot be unicode
    __all__ = [n.encode('ascii') for n in __all__]
