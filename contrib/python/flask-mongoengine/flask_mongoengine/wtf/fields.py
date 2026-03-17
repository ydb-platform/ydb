"""
Useful form fields for use with the mongoengine.
"""
import json
import sys
from gettext import gettext as _

from mongoengine.queryset import DoesNotExist
from wtforms import widgets
from wtforms.fields import SelectFieldBase, StringField, TextAreaField
from wtforms.validators import ValidationError

__all__ = (
    "ModelSelectField",
    "QuerySetSelectField",
)

if sys.version_info >= (3, 0):
    unicode = str


class QuerySetSelectField(SelectFieldBase):
    """
    Given a QuerySet either at initialization or inside a view, will display a
    select drop-down field of choices. The `data` property actually will
    store/keep an ORM model instance, not the ID. Submitting a choice which is
    not in the queryset will result in a validation error.

    Specifying `label_attr` in the constructor will use that property of the
    model instance for display in the list, else the model object's `__str__`
    or `__unicode__` will be used.

    If `allow_blank` is set to `True`, then a blank choice will be added to the
    top of the list. Selecting this choice will result in the `data` property
    being `None`.  The label for the blank choice can be set by specifying the
    `blank_text` parameter.
    """

    widget = widgets.Select()

    def __init__(
        self,
        label="",
        validators=None,
        queryset=None,
        label_attr="",
        allow_blank=False,
        blank_text="---",
        label_modifier=None,
        **kwargs,
    ):

        super(QuerySetSelectField, self).__init__(label, validators, **kwargs)
        self.label_attr = label_attr
        self.allow_blank = allow_blank
        self.blank_text = blank_text
        self.label_modifier = label_modifier
        self.queryset = queryset

    def iter_choices(self):
        if self.allow_blank:
            yield ("__None", self.blank_text, self.data is None)

        if self.queryset is None:
            return

        self.queryset.rewind()
        for obj in self.queryset:
            label = (
                self.label_modifier(obj)
                if self.label_modifier
                else (self.label_attr and getattr(obj, self.label_attr) or obj)
            )

            if isinstance(self.data, list):
                selected = obj in self.data
            else:
                selected = self._is_selected(obj)
            yield (obj.id, label, selected)

    def process_formdata(self, valuelist):
        if valuelist:
            if valuelist[0] == "__None":
                self.data = None
            else:
                if self.queryset is None:
                    self.data = None
                    return

                try:
                    obj = self.queryset.get(pk=valuelist[0])
                    self.data = obj
                except DoesNotExist:
                    self.data = None

    def pre_validate(self, form):
        if not self.allow_blank or self.data is not None:
            if not self.data:
                raise ValidationError(_("Not a valid choice"))

    def _is_selected(self, item):
        return item == self.data


class QuerySetSelectMultipleField(QuerySetSelectField):

    widget = widgets.Select(multiple=True)

    def __init__(
        self,
        label="",
        validators=None,
        queryset=None,
        label_attr="",
        allow_blank=False,
        blank_text="---",
        **kwargs,
    ):

        super(QuerySetSelectMultipleField, self).__init__(
            label, validators, queryset, label_attr, allow_blank, blank_text, **kwargs
        )

    def process_formdata(self, valuelist):

        if valuelist:
            if valuelist[0] == "__None":
                self.data = None
            else:
                if not self.queryset:
                    self.data = None
                    return

                self.queryset.rewind()
                self.data = list(self.queryset(pk__in=valuelist))
                if not len(self.data):
                    self.data = None

        # If no value passed, empty the list
        else:
            self.data = None

    def _is_selected(self, item):
        return item in self.data if self.data else False


class ModelSelectField(QuerySetSelectField):
    """
    Like a QuerySetSelectField, except takes a model class instead of a
    queryset and lists everything in it.
    """

    def __init__(self, label="", validators=None, model=None, **kwargs):
        queryset = kwargs.pop("queryset", model.objects)
        super(ModelSelectField, self).__init__(
            label, validators, queryset=queryset, **kwargs
        )


class ModelSelectMultipleField(QuerySetSelectMultipleField):
    """
    Allows multiple select
    """

    def __init__(self, label="", validators=None, model=None, **kwargs):
        queryset = kwargs.pop("queryset", model.objects)
        super(ModelSelectMultipleField, self).__init__(
            label, validators, queryset=queryset, **kwargs
        )


class JSONField(TextAreaField):
    def _value(self):
        if self.raw_data:
            return self.raw_data[0]
        else:
            return self.data and unicode(json.dumps(self.data)) or ""

    def process_formdata(self, value):
        if value:
            try:
                self.data = json.loads(value[0])
            except ValueError:
                raise ValueError(self.gettext("Invalid JSON data."))


class DictField(JSONField):
    def process_formdata(self, value):
        super(DictField, self).process_formdata(value)
        if value and not isinstance(self.data, dict):
            raise ValueError(self.gettext("Not a valid dictionary."))


class NoneStringField(StringField):
    """
    Custom StringField that counts "" as None
    """

    def process_formdata(self, valuelist):
        if valuelist:
            self.data = valuelist[0]
        if self.data == "":
            self.data = None


class BinaryField(TextAreaField):
    """
    Custom TextAreaField that converts its value with binary_type.
    """

    def process_formdata(self, valuelist):
        if valuelist:
            self.data = bytes(valuelist[0], "utf-8")
