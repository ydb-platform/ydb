from django.utils.encoding import force_str
from django.utils.translation import gettext_lazy as _
from rest_framework.fields import CharField

from timezone_field.backends import TimeZoneNotFoundError, get_tz_backend


class TimeZoneSerializerField(CharField):
    default_error_messages = {
        "invalid": _("A valid timezone is required."),
    }

    def __init__(self, *args, **kwargs):
        self.use_pytz = kwargs.pop("use_pytz", None)
        self.tz_backend = get_tz_backend(use_pytz=self.use_pytz)
        super().__init__(*args, **kwargs)

    def to_internal_value(self, data):
        data_str = force_str(data)
        try:
            return self.tz_backend.to_tzobj(data_str)
        except TimeZoneNotFoundError:
            self.fail("invalid")

    def to_representation(self, value):
        return str(value)
