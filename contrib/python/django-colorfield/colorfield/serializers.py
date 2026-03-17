from django.core.exceptions import ValidationError as DjangoValidationError

try:
    from rest_framework.serializers import CharField
    from rest_framework.serializers import ValidationError as DRFValidationError
except ImportError:
    ModuleNotFoundError("Django REST Framework is not installed.")

from colorfield.validators import (
    color_hex_validator,
    color_hexa_validator,
    color_rgb_validator,
    color_rgba_validator,
)


class ColorField(CharField):
    default_error_messages = {
        "invalid": [
            color_hex_validator.message,
            color_hexa_validator.message,
            color_rgb_validator.message,
            color_rgba_validator.message,
        ]
    }

    def to_internal_value(self, data):
        errors = {
            "hex": False,
            "hexa": False,
            "rgb": False,
            "rgba": False,
        }
        try:
            color_hex_validator(data)
        except DjangoValidationError:
            errors["hex"] = True

        try:
            color_hexa_validator(data)
        except DjangoValidationError:
            errors["hexa"] = True

        try:
            color_rgb_validator(data)
        except DjangoValidationError:
            errors["rgb"] = True

        try:
            color_rgba_validator(data)
        except DjangoValidationError:
            errors["rgba"] = True

        if all(errors.values()):
            raise DRFValidationError(self.default_error_messages.get("invalid"))

        return data
