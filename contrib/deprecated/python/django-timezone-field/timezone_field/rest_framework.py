import pytz
from django.utils.translation import gettext_lazy as _
from django.utils.encoding import force_str
from rest_framework.fields import Field


class TimeZoneSerializerField(Field):
    default_error_messages = {
        'invalid': _('A valid timezone is required.'),
    }

    def to_internal_value(self, data):
        try:
            return pytz.timezone(force_str(data))
        except pytz.UnknownTimeZoneError:
            self.fail('invalid')

    def to_representation(self, value):
        return str(value)
