from django import forms
from django.db.models import Field
import numpy as np
from .. import Vector


# https://docs.djangoproject.com/en/5.0/howto/custom-model-fields/
class VectorField(Field):
    description = 'Vector'
    empty_strings_allowed = False

    def __init__(self, *args, dimensions=None, **kwargs):
        self.dimensions = dimensions
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if self.dimensions is not None:
            kwargs['dimensions'] = self.dimensions
        return name, path, args, kwargs

    def db_type(self, connection):
        if self.dimensions is None:
            return 'vector'
        return 'vector(%d)' % self.dimensions

    def from_db_value(self, value, expression, connection):
        return Vector._from_db(value)

    def to_python(self, value):
        if isinstance(value, list):
            return np.array(value, dtype=np.float32)
        return Vector._from_db(value)

    def get_prep_value(self, value):
        return Vector._to_db(value)

    def value_to_string(self, obj):
        return self.get_prep_value(self.value_from_object(obj))

    def validate(self, value, model_instance):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        super().validate(value, model_instance)

    def run_validators(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        super().run_validators(value)

    def formfield(self, **kwargs):
        return super().formfield(form_class=VectorFormField, **kwargs)


class VectorWidget(forms.TextInput):
    def format_value(self, value):
        if isinstance(value, np.ndarray):
            value = value.tolist()
        return super().format_value(value)


class VectorFormField(forms.CharField):
    widget = VectorWidget

    def has_changed(self, initial, data):
        if isinstance(initial, np.ndarray):
            initial = initial.tolist()
        return super().has_changed(initial, data)

    def to_python(self, value):
        if isinstance(value, str) and value == '':
            return None
        return super().to_python(value)
