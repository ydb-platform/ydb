"""
Tools for generating forms based on Peewee models
(cribbed from wtforms.ext.django)
"""

from collections import namedtuple
from collections import OrderedDict
from wtforms import __version__ as wtforms_version
from wtforms import Form
from wtforms import fields as f
from wtforms import validators
from wtfpeewee.fields import ModelSelectField
from wtfpeewee.fields import SelectChoicesField
from wtfpeewee.fields import SelectQueryField
from wtfpeewee.fields import WPDateField
from wtfpeewee.fields import WPDateTimeField
from wtfpeewee.fields import WPTimeField
from wtfpeewee.fields import WPJSONAreaField
from wtfpeewee._compat import text_type

from peewee import BareField
from peewee import BigIntegerField
from peewee import BlobField
from peewee import BooleanField
from peewee import CharField
from peewee import DateField
from peewee import DateTimeField
from peewee import DecimalField
from peewee import DoubleField
from peewee import FloatField
from peewee import ForeignKeyField
from peewee import IntegerField
from peewee import IPField
from peewee import AutoField
from peewee import SmallIntegerField
from peewee import TextField
from peewee import TimeField
from peewee import TimestampField
from peewee import UUIDField

from playhouse.postgres_ext import JSONField as PostgresJSONField
from playhouse.postgres_ext import BinaryJSONField as PostgresBinaryJSONField
from playhouse.sqlite_ext import JSONField as SQLiteJSONField


__all__ = (
    'FieldInfo',
    'ModelConverter',
    'model_fields',
    'model_form')

def handle_null_filter(data):
    if data == '':
        return None
    return data

class ValueRequired(object):
    """
    Custom validation class that differentiates between false-y values and
    truly blank values. See the implementation of DataRequired and
    InputRequired -- this class sits somewhere in the middle of them.
    """
    if wtforms_version < '3.1.0':
        field_flags = ('required',)
    else:
        field_flags = {'required': True}

    def __init__(self, message=None):
        self.message = message

    def __call__(self, form, field):
        if field.data is None or isinstance(field.data, text_type) \
           and not field.data.strip():
            message = self.message or field.gettext('This field is required.')
            field.errors[:] = []
            raise validators.StopValidation(message)

FieldInfo = namedtuple('FieldInfo', ('name', 'field'))

class ModelConverter(object):
    defaults = OrderedDict((
        # Subclasses of other fields.
        (IPField, f.StringField),  # Subclass of BigIntegerField.
        (TimestampField, WPDateTimeField),  # Subclass of BigIntegerField.
        (AutoField, f.HiddenField),
        (BigIntegerField, f.IntegerField),
        (DoubleField, f.FloatField),
        (SmallIntegerField, f.IntegerField),
        (SQLiteJSONField, WPJSONAreaField),

        # Base-classes.
        (BareField, f.StringField),
        (BlobField, f.TextAreaField),
        (BooleanField, f.BooleanField),
        (CharField, f.StringField),
        (DateField, WPDateField),
        (DateTimeField, WPDateTimeField),
        (DecimalField, f.DecimalField),
        (FloatField, f.FloatField),
        (IntegerField, f.IntegerField),
        (TextField, f.TextAreaField),
        (TimeField, WPTimeField),
        (UUIDField, f.StringField),

        (PostgresJSONField, WPJSONAreaField),
        (PostgresBinaryJSONField, WPJSONAreaField),
    ))
    coerce_defaults = {
        BigIntegerField: int,
        CharField: text_type,
        DoubleField: float,
        FloatField: float,
        IntegerField: int,
        SmallIntegerField: int,
        TextField: text_type,
        UUIDField: text_type,
    }

    def __init__(self, additional=None, additional_coerce=None, overrides=None):
        self.converters = {ForeignKeyField: self.handle_foreign_key}
        if additional:
            self.converters.update(additional)

        self.coerce_settings = dict(self.coerce_defaults)
        if additional_coerce:
            self.coerce_settings.update(additional_coerce)

        self.overrides = overrides or {}

    def handle_foreign_key(self, model, field, **kwargs):
        if field.null:
            kwargs['allow_blank'] = True
        if field.choices is not None:
            field_obj = SelectQueryField(query=field.choices, **kwargs)
        else:
            field_obj = ModelSelectField(model=field.rel_model, **kwargs)
        return FieldInfo(field.name, field_obj)

    def convert(self, model, field, field_args):
        kwargs = {
            'label': field.verbose_name,
            'validators': [],
            'filters': [],
            'default': field.default,
            'description': field.help_text}
        if field_args:
            kwargs.update(field_args)

        if kwargs['validators']:
            # Create a copy of the list since we will be modifying it.
            kwargs['validators'] = list(kwargs['validators'])

        if field.null:
            # Treat empty string as None when converting.
            kwargs['filters'].append(handle_null_filter)

        if (field.null or (field.default is not None)) or (
                field.choices and any(not (v) for v, _ in field.choices)):
            # We allow the field to be optional if:
            # 1. the field is null=True and can be blank.
            # 2. the field has a default value.
            kwargs['validators'].append(validators.Optional())
        else:
            kwargs['validators'].append(ValueRequired())

        if field.name in self.overrides:
            return FieldInfo(field.name, self.overrides[field.name](**kwargs))

        # Allow custom-defined Peewee field classes to define their own conversion,
        # making it so that code which calls model_form() doesn't have to have special
        # cases, especially when called for the same peewee.Model from multiple places, or
        # when called in a generic context which the end-developer has less control over,
        # such as via flask-admin.
        if hasattr(field, 'wtf_field'):
            return FieldInfo(field.name, field.wtf_field(model, **kwargs))

        for converter in self.converters:
            if isinstance(field, converter):
                return self.converters[converter](model, field, **kwargs)
        else:
            for converter in self.defaults:
                if not isinstance(field, converter):
                    # Early-continue because it simplifies reading the following code.
                    continue
                if issubclass(self.defaults[converter], f.FormField):
                    # FormField fields (i.e. for nested forms) do not support
                    # filters.
                    kwargs.pop('filters')
                if field.choices or 'choices' in kwargs:
                    choices = kwargs.pop('choices', field.choices)
                    if converter in self.coerce_settings or 'coerce' in kwargs:
                        coerce_fn = kwargs.pop('coerce',
                                               self.coerce_settings[converter])
                        allow_blank = kwargs.pop('allow_blank', field.null)
                        kwargs.update({
                            'choices': choices,
                            'coerce': coerce_fn,
                            'allow_blank': allow_blank})

                        return FieldInfo(field.name, SelectChoicesField(**kwargs))

                return FieldInfo(field.name, self.defaults[converter](**kwargs))

        raise AttributeError("There is not possible conversion "
                             "for '%s'" % type(field))


def model_fields(model, allow_pk=False, only=None, exclude=None,
                 field_args=None, converter=None):
    """
    Generate a dictionary of fields for a given Peewee model.

    See `model_form` docstring for description of parameters.
    """
    converter = converter or ModelConverter()
    field_args = field_args or {}

    model_fields = list(model._meta.sorted_fields)
    if not allow_pk:
        model_fields.pop(0)

    if only:
        model_fields = [x for x in model_fields if x.name in only]
    elif exclude:
        model_fields = [x for x in model_fields if x.name not in exclude]

    field_dict = {}
    for model_field in model_fields:
        name, field = converter.convert(
            model,
            model_field,
            field_args.get(model_field.name))
        field_dict[name] = field

    return field_dict


def model_form(model, base_class=Form, allow_pk=False, only=None, exclude=None,
               field_args=None, converter=None):
    """
    Create a wtforms Form for a given Peewee model class::

        from wtfpeewee.orm import model_form
        from myproject.myapp.models import User
        UserForm = model_form(User)

    :param model:
        A Peewee model class
    :param base_class:
        Base form class to extend from. Must be a ``wtforms.Form`` subclass.
    :param only:
        An optional iterable with the property names that should be included in
        the form. Only these properties will have fields.
    :param exclude:
        An optional iterable with the property names that should be excluded
        from the form. All other properties will have fields.
    :param field_args:
        An optional dictionary of field names mapping to keyword arguments used
        to construct each field object.
    :param converter:
        A converter to generate the fields based on the model properties. If
        not set, ``ModelConverter`` is used.
    """
    field_dict = model_fields(model, allow_pk, only, exclude, field_args, converter)
    return type(model.__name__ + 'Form', (base_class, ), field_dict)
