from django import forms
from django.core.exceptions import ValidationError

from timezone_field.backends import TimeZoneNotFoundError, get_tz_backend
from timezone_field.choices import standard, with_gmt_offset


def get_coerce(tz_backend):
    def coerce(val):
        try:
            return tz_backend.to_tzobj(val)
        except TimeZoneNotFoundError as err:
            raise ValidationError(f"Unknown time zone: '{val}'") from err

    return coerce


class TimeZoneFormField(forms.TypedChoiceField):
    def __init__(self, *args, **kwargs):
        self.use_pytz = kwargs.pop("use_pytz", None)
        self.tz_backend = get_tz_backend(self.use_pytz)
        kwargs.setdefault("coerce", get_coerce(self.tz_backend))
        kwargs.setdefault("empty_value", None)

        if "choices" in kwargs:
            values, displays = zip(*kwargs["choices"])
        else:
            values = self.tz_backend.base_tzstrs
            displays = None

        choices_display = kwargs.pop("choices_display", None)
        if choices_display == "WITH_GMT_OFFSET":
            choices = with_gmt_offset(values, use_pytz=self.use_pytz)
        elif choices_display == "STANDARD":
            choices = standard(values)
        elif choices_display is None:
            choices = zip(values, displays) if displays else standard(values)
        else:
            raise ValueError(f"Unrecognized value for kwarg 'choices_display' of '{choices_display}'")

        kwargs["choices"] = choices
        super().__init__(*args, **kwargs)
