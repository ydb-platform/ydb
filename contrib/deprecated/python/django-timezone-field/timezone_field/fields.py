import pytz

from django.core.exceptions import ValidationError
from django.db import models
from django.utils.encoding import force_str

from timezone_field.choices import standard, with_gmt_offset
from timezone_field.utils import is_pytz_instance, add_gmt_offset_to_choices


class TimeZoneField(models.Field):
    """
    Provides database store for pytz timezone objects.

    Valid inputs:
        * any instance of pytz.tzinfo.DstTzInfo or pytz.tzinfo.StaticTzInfo
        * the pytz.UTC singleton
        * any string that validates against pytz.common_timezones. pytz will
          be used to build a timezone object from the string.
        * None and the empty string both represent 'no timezone'

    Valid outputs:
        * None
        * instances of pytz.tzinfo.DstTzInfo and pytz.tzinfo.StaticTzInfo
        * the pytz.UTC singleton

    Blank values are stored in the DB as the empty string. Timezones are stored
    in their string representation.

    The `choices` kwarg can be specified as a list of either
    [<pytz.timezone>, <str>] or [<str>, <str>]. Internally, it is stored as
    [<pytz.timezone>, <str>].
    """

    description = "A pytz timezone object"

    # NOTE: these defaults are excluded from migrations. If these are changed,
    #       existing migration files will need to be accomodated.
    default_tzs = [pytz.timezone(tz) for tz in pytz.common_timezones]
    default_choices = standard(default_tzs)
    default_max_length = 63

    def __init__(self, *args, **kwargs):
        # allow some use of positional args up until the args we customize
        # https://github.com/mfogel/django-timezone-field/issues/42
        # https://github.com/django/django/blob/1.11.11/django/db/models/fields/__init__.py#L145
        if len(args) > 3:
            raise ValueError('Cannot specify max_length by positional arg')
        kwargs.setdefault('max_length', self.default_max_length)

        if 'choices' in kwargs:
            values, displays = zip(*kwargs['choices'])
            # Choices can be specified in two forms: either
            # [<pytz.timezone>, <str>] or [<str>, <str>]
            #
            # The [<pytz.timezone>, <str>] format is the one we actually
            # store the choices in memory because of
            # https://github.com/mfogel/django-timezone-field/issues/24
            #
            # The [<str>, <str>] format is supported because since django
            # can't deconstruct pytz.timezone objects, migration files must
            # use an alternate format. Representing the timezones as strings
            # is the obvious choice.
            if not is_pytz_instance(values[0]):
                values = [pytz.timezone(v) for v in values]
        else:
            values = self.default_tzs
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

        # 'display_GMT_offset' is deprecated, use 'choices_display' instead
        if kwargs.pop('display_GMT_offset', False):
            choices = add_gmt_offset_to_choices(choices)

        kwargs['choices'] = choices
        super(TimeZoneField, self).__init__(*args, **kwargs)

    def validate(self, value, model_instance):
        if not is_pytz_instance(value):
            raise ValidationError("'%s' is not a pytz timezone object" % value)
        super(TimeZoneField, self).validate(value, model_instance)

    def deconstruct(self):
        name, path, args, kwargs = super(TimeZoneField, self).deconstruct()
        if kwargs.get('choices') == self.default_choices:
            del kwargs['choices']
        if kwargs.get('max_length') == self.default_max_length:
            del kwargs['max_length']

        # django can't decontruct pytz objects, so transform choices
        # to [<str>, <str>] format for writing out to the migration
        if 'choices' in kwargs:
            kwargs['choices'] = [(tz.zone, n) for tz, n in kwargs['choices']]
        return name, path, args, kwargs

    def get_internal_type(self):
        return 'CharField'

    def get_default(self):
        # allow defaults to be still specified as strings. Allows for easy
        # serialization into migration files
        value = super(TimeZoneField, self).get_default()
        return self._get_python_and_db_repr(value)[0]

    def from_db_value(self, value, *args):
        "Convert to pytz timezone object"
        return self._get_python_and_db_repr(value)[0]

    def to_python(self, value):
        "Convert to pytz timezone object"
        return self._get_python_and_db_repr(value)[0]

    def get_prep_value(self, value):
        "Convert to string describing a valid pytz timezone object"
        return self._get_python_and_db_repr(value)[1]

    def _get_python_and_db_repr(self, value):
        "Returns a tuple of (python representation, db representation)"
        if value is None or value == '':
            return (None, '')
        if is_pytz_instance(value):
            return (value, value.zone)
        try:
            return (pytz.timezone(force_str(value)), force_str(value))
        except pytz.UnknownTimeZoneError:
            pass
        raise ValidationError("Invalid timezone '%s'" % value)
