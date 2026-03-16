import json
from typing import Optional

from django.core.serializers.json import DjangoJSONEncoder
from django.forms import BoundField, Field, ModelChoiceField

from .formsets import BaseFormSet
from .widgets import SubformWidget

if False:  # pragma: nocover
    from .base import TypeSubform  # noqa


class EnhancedBoundField(BoundField):
    """This custom bound field allows widgets to access the field itself."""

    def as_widget(self, widget=None, attrs=None, only_initial=False):
        widget = widget or self.field.widget
        widget.bound_field = self
        return super().as_widget(widget, attrs, only_initial)


class EnhancedField(Field):
    """This custom field offers improvements over the base one."""

    def get_bound_field(self, form, field_name):
        return EnhancedBoundField(form, self, field_name)


class SubformField(EnhancedField):
    """Field representing a subform."""

    widget = SubformWidget

    def __init__(self, *args, original_field, **kwargs):
        super().__init__(*args, **kwargs)
        self.original_field = original_field
        self.form: Optional['TypeSubform'] = None

        # todo Maybe proxy other attributes?
        self.label = original_field.label
        self.help_text = original_field.help_text
        self.to_python = original_field.to_python

    @classmethod
    def _json_serialize(cls, value: dict) -> str:
        return json.dumps(value, cls=DjangoJSONEncoder)

    def clean(self, value):
        original_field = self.original_field

        if isinstance(original_field, ModelChoiceField):
            form = self.form

            if isinstance(form, BaseFormSet):
                value_ = []

                for item in value or []:
                    item_id = item.get('id')

                    if item_id is None:
                        # New m2m item is to be initialized on fly.
                        # todo maybe this should be opt-out
                        for extra_form in form.extra_forms:
                            # Try to find an exact form which produced the data.
                            if extra_form.cleaned_data is item:
                                instance = extra_form.save()
                                if instance.pk:
                                    item_id = instance.id
                                    item['id'] = instance

                    if item_id:
                        # item id here is actually a model instance
                        value_.append(item['id'].id)

                value = value_

            else:
                # For a subform with a model (FK).
                value = form.initial.get('id')

                if value is None and form.instance.pk is None:
                    # New foreign key item is to be initialized on fly.
                    # todo maybe this should be opt-out
                    instance = form.save()
                    if instance.pk:
                        return instance

        else:
            # For a subform with JSON this `value` contains `cleaned_data` dictionary.
            # We convert this into json to allow parent form field to clean it.
            value = self._json_serialize(value)

        return original_field.clean(value)
