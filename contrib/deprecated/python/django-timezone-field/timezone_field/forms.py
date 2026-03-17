import pytz
from django.core.exceptions import ValidationError
from django import forms

from timezone_field.choices import standard, with_gmt_offset


def coerce_to_pytz(val):
    try:
        return pytz.timezone(val)
    except pytz.UnknownTimeZoneError:
        raise ValidationError("Unknown time zone: '%s'" % val)


class TimeZoneFormField(forms.TypedChoiceField):

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('coerce', coerce_to_pytz)
        kwargs.setdefault('empty_value', None)

        if 'choices' in kwargs:
            values, displays = zip(*kwargs['choices'])
        else:
            values = pytz.common_timezones
            displays = None

        choices_display = kwargs.pop('choices_display', None)
        if choices_display == 'WITH_GMT_OFFSET':
            choices = with_gmt_offset(values)
        elif choices_display == 'STANDARD':
            choices = standard(values)
        elif choices_display is None:
            choices = zip(values, displays) if displays else standard(values)
        else:
            raise ValueError(
                "Unrecognized value for kwarg 'choices_display' of '"
                + choices_display + "'"
            )

        kwargs['choices'] = choices
        super(TimeZoneFormField, self).__init__(*args, **kwargs)
