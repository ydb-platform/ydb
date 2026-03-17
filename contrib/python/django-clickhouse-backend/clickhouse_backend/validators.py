from django.core.validators import BaseValidator
from django.utils.deconstruct import deconstructible
from django.utils.translation import ngettext_lazy


@deconstructible
class MaxBytesValidator(BaseValidator):
    message = ngettext_lazy(
        "Ensure this value has at most %(limit_value)d byte (it has %(show_value)d).",
        "Ensure this value has at most %(limit_value)d bytes (it has %(show_value)d).",
        "limit_value",
    )
    code = "max_bytes"

    def compare(self, a, b):
        return a > b

    def clean(self, x):
        if isinstance(x, str):
            x = x.encode("utf-8")
        return len(x)
