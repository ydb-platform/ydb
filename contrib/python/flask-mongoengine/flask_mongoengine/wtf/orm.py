"""
Tools for generating forms based on mongoengine Document schemas.
"""
import decimal
import sys
from collections import OrderedDict

from bson import ObjectId
from mongoengine import ReferenceField
from wtforms import fields as f, validators

from flask_mongoengine.wtf.fields import (
    BinaryField,
    DictField,
    ModelSelectField,
    ModelSelectMultipleField,
    NoneStringField,
)
from flask_mongoengine.wtf.models import ModelForm

__all__ = (
    "model_fields",
    "model_form",
)


if sys.version_info >= (3, 0):
    unicode = str


def converts(*args):
    def _inner(func):
        func._converter_for = frozenset(args)
        return func

    return _inner


class ModelConverter(object):
    def __init__(self, converters=None):
        if not converters:
            converters = {}

        for name in dir(self):
            obj = getattr(self, name)
            if hasattr(obj, "_converter_for"):
                for classname in obj._converter_for:
                    converters[classname] = obj

        self.converters = converters

    def convert(self, model, field, field_args):
        kwargs = {
            "label": getattr(field, "verbose_name", field.name),
            "description": getattr(field, "help_text", None) or "",
            "validators": getattr(field, "validators", None) or [],
            "filters": getattr(field, "filters", None) or [],
            "default": field.default,
        }
        if field_args:
            kwargs.update(field_args)

        if kwargs["validators"]:
            # Create a copy of the list since we will be modifying it.
            kwargs["validators"] = list(kwargs["validators"])

        if field.required:
            kwargs["validators"].append(validators.InputRequired())
        else:
            kwargs["validators"].append(validators.Optional())

        ftype = type(field).__name__

        if field.choices:
            kwargs["choices"] = field.choices

            if ftype in self.converters:
                kwargs["coerce"] = self.coerce(ftype)
            multiple_field = kwargs.pop("multiple", False)
            radio_field = kwargs.pop("radio", False)
            if multiple_field:
                return f.SelectMultipleField(**kwargs)
            if radio_field:
                return f.RadioField(**kwargs)
            return f.SelectField(**kwargs)

        ftype = type(field).__name__

        if hasattr(field, "to_form_field"):
            return field.to_form_field(model, kwargs)

        if hasattr(field, "field") and type(field.field) == ReferenceField:
            kwargs["label_modifier"] = getattr(
                model, field.name + "_label_modifier", None
            )

        if ftype in self.converters:
            return self.converters[ftype](model, field, kwargs)

    @classmethod
    def _string_common(cls, model, field, kwargs):
        if field.max_length or field.min_length:
            kwargs["validators"].append(
                validators.Length(
                    max=field.max_length or -1, min=field.min_length or -1
                )
            )

    @classmethod
    def _number_common(cls, model, field, kwargs):
        if field.max_value or field.min_value:
            kwargs["validators"].append(
                validators.NumberRange(max=field.max_value, min=field.min_value)
            )

    @converts("StringField")
    def conv_String(self, model, field, kwargs):
        if field.regex:
            kwargs["validators"].append(validators.Regexp(regex=field.regex))
        self._string_common(model, field, kwargs)
        password_field = kwargs.pop("password", False)
        textarea_field = kwargs.pop("textarea", False) or not field.max_length
        if password_field:
            return f.PasswordField(**kwargs)
        if textarea_field:
            return f.TextAreaField(**kwargs)
        return f.StringField(**kwargs)

    @converts("URLField")
    def conv_URL(self, model, field, kwargs):
        kwargs["validators"].append(validators.URL())
        self._string_common(model, field, kwargs)
        return NoneStringField(**kwargs)

    @converts("EmailField")
    def conv_Email(self, model, field, kwargs):
        kwargs["validators"].append(validators.Email())
        self._string_common(model, field, kwargs)
        return NoneStringField(**kwargs)

    @converts("IntField")
    def conv_Int(self, model, field, kwargs):
        self._number_common(model, field, kwargs)
        return f.IntegerField(**kwargs)

    @converts("FloatField")
    def conv_Float(self, model, field, kwargs):
        self._number_common(model, field, kwargs)
        return f.FloatField(**kwargs)

    @converts("DecimalField")
    def conv_Decimal(self, model, field, kwargs):
        self._number_common(model, field, kwargs)
        kwargs["places"] = getattr(field, "precision", None)
        return f.DecimalField(**kwargs)

    @converts("BooleanField")
    def conv_Boolean(self, model, field, kwargs):
        return f.BooleanField(**kwargs)

    @converts("DateTimeField")
    def conv_DateTime(self, model, field, kwargs):
        return f.DateTimeField(**kwargs)

    @converts("DateField")
    def conv_Date(self, model, field, kwargs):
        return f.DateField(**kwargs)

    @converts("BinaryField")
    def conv_Binary(self, model, field, kwargs):
        # TODO: may be set file field that will save file`s data to MongoDB
        if field.max_bytes:
            kwargs["validators"].append(validators.Length(max=field.max_bytes))
        return BinaryField(**kwargs)

    @converts("DictField")
    def conv_Dict(self, model, field, kwargs):
        return DictField(**kwargs)

    @converts("ListField")
    def conv_List(self, model, field, kwargs):
        if isinstance(field.field, ReferenceField):
            return ModelSelectMultipleField(model=field.field.document_type, **kwargs)
        if field.field.choices:
            kwargs["multiple"] = True
            return self.convert(model, field.field, kwargs)
        field_args = kwargs.pop("field_args", {})
        unbound_field = self.convert(model, field.field, field_args)
        unacceptable = {
            "validators": [],
            "filters": [],
            "min_entries": kwargs.get("min_entries", 0),
        }
        kwargs.update(unacceptable)
        return f.FieldList(unbound_field, **kwargs)

    @converts("SortedListField")
    def conv_SortedList(self, model, field, kwargs):
        # TODO: sort functionality, may be need sortable widget
        return self.conv_List(model, field, kwargs)

    @converts("GeoLocationField")
    def conv_GeoLocation(self, model, field, kwargs):
        # TODO: create geo field and widget (also GoogleMaps)
        return

    @converts("ObjectIdField")
    def conv_ObjectId(self, model, field, kwargs):
        return

    @converts("EmbeddedDocumentField")
    def conv_EmbeddedDocument(self, model, field, kwargs):
        kwargs = {
            "validators": [],
            "filters": [],
            "default": field.default or field.document_type_obj,
        }
        form_class = model_form(field.document_type_obj, field_args={})
        return f.FormField(form_class, **kwargs)

    @converts("ReferenceField")
    def conv_Reference(self, model, field, kwargs):
        return ModelSelectField(model=field.document_type, **kwargs)

    @converts("GenericReferenceField")
    def conv_GenericReference(self, model, field, kwargs):
        return

    def coerce(self, field_type):
        coercions = {
            "IntField": int,
            "BooleanField": bool,
            "FloatField": float,
            "DecimalField": decimal.Decimal,
            "ObjectIdField": ObjectId,
        }
        return coercions.get(field_type, unicode)


def model_fields(model, only=None, exclude=None, field_args=None, converter=None):
    """
    Generate a dictionary of fields for a given database model.

    See `model_form` docstring for description of parameters.
    """
    from mongoengine.base import BaseDocument, DocumentMetaclass

    if not isinstance(model, (BaseDocument, DocumentMetaclass)):
        raise TypeError("model must be a mongoengine Document schema")

    converter = converter or ModelConverter()
    field_args = field_args or {}

    names = ((k, v.creation_counter) for k, v in model._fields.items())
    field_names = [n[0] for n in sorted(names, key=lambda n: n[1])]

    if only:
        field_names = [x for x in only if x in set(field_names)]
    elif exclude:
        field_names = [x for x in field_names if x not in set(exclude)]

    field_dict = OrderedDict()
    for name in field_names:
        model_field = model._fields[name]
        field = converter.convert(model, model_field, field_args.get(name))
        if field is not None:
            field_dict[name] = field

    return field_dict


def model_form(
    model,
    base_class=ModelForm,
    only=None,
    exclude=None,
    field_args=None,
    converter=None,
):
    """
    Create a wtforms Form for a given mongoengine Document schema::

        from flask_mongoengine.wtf import model_form
        from myproject.myapp.schemas import Article
        ArticleForm = model_form(Article)

    :param model:
        A mongoengine Document schema class
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
    field_dict = model_fields(model, only, exclude, field_args, converter)
    field_dict["model_class"] = model
    return type(model.__name__ + "Form", (base_class,), field_dict)
