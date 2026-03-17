# -- encoding: UTF-8 --
from __future__ import absolute_import

from django.forms import TypedChoiceField
from django.forms.fields import TypedMultipleChoiceField
from django.utils.encoding import force_text

from .enums import Enum

__all__ = ["EnumChoiceField", "EnumMultipleChoiceField"]


class EnumChoiceFieldMixin(object):
    def prepare_value(self, value):
        # Widgets expect to get strings as values.

        if value is None:
            return ''
        if hasattr(value, "value"):
            value = value.value
        return force_text(value)

    def valid_value(self, value):
        if hasattr(value, "value"):  # Try validation using the enum value first.
            if super(EnumChoiceFieldMixin, self).valid_value(value.value):
                return True
        return super(EnumChoiceFieldMixin, self).valid_value(value)

    def to_python(self, value):
        if isinstance(value, Enum):
            value = value.value
        return super(EnumChoiceFieldMixin, self).to_python(value)


class EnumChoiceField(EnumChoiceFieldMixin, TypedChoiceField):
    pass


class EnumMultipleChoiceField(EnumChoiceFieldMixin, TypedMultipleChoiceField):
    pass
