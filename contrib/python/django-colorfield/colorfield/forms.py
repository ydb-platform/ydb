from django import forms

from colorfield.validators import color_hex_validator
from colorfield.widgets import ColorWidget


class ColorField(forms.CharField):
    default_validators = [
        color_hex_validator,
    ]

    def __init__(self, *args, **kwargs):
        validator = kwargs.pop("validator", color_hex_validator)
        self.default_validators = [validator]
        kwargs.setdefault("widget", ColorWidget())
        super().__init__(*args, **kwargs)
