import datetime
import re
from functools import wraps
from ..datetime_helpers import parse_datetime
try:
    basestring
except NameError:
    # In case we are in python 3 the string check is different
    basestring = str


def property_is_enum(enum_type):
    def property_type_decorator(func):
        @wraps(func)
        def wrapper(self, value):
            if value is not None and not hasattr(enum_type, value):
                error = "Invalid value: {0}. {1} must be of type {2}.".format(value, func.__name__, enum_type.__name__)
                raise ValueError(error)
            return func(self, value)

        return wrapper

    return property_type_decorator


def property_is_boolean(func):
    @wraps(func)
    def wrapper(self, value):
        if not isinstance(value, bool):
            error = "Boolean expected for {0} flag.".format(func.__name__)
            raise ValueError(error)
        return func(self, value)

    return wrapper


def property_not_nullable(func):
    @wraps(func)
    def wrapper(self, value):
        if value is None:
            error = "{0} must be defined.".format(func.__name__)
            raise ValueError(error)
        return func(self, value)

    return wrapper


def property_not_empty(func):
    @wraps(func)
    def wrapper(self, value):
        if not value:
            error = "{0} must not be empty.".format(func.__name__)
            raise ValueError(error)
        return func(self, value)

    return wrapper


def property_is_valid_time(func):
    @wraps(func)
    def wrapper(self, value):
        units_of_time = {"hour", "minute", "second"}

        if not any(hasattr(value, unit) for unit in units_of_time):
            error = "Invalid time object defined."
            raise ValueError(error)
        return func(self, value)

    return wrapper


def property_is_int(range, allowed=None):
    '''Takes a range of ints and a list of exemptions to check against
    when setting a property on a model. The range is a tuple of (min, max) and the
    allowed list (empty by default) allows values outside that range.
    This is useful for when we use sentinel values.

    Example: Revisions allow a range of 2-10000, but use -1 as a sentinel for 'unlimited'.
    '''

    if allowed is None:
        allowed = ()  # Empty tuple for fast no-op testing.

    def property_type_decorator(func):
        @wraps(func)
        def wrapper(self, value):
            error = "Invalid property defined: '{}'. Integer value expected.".format(value)

            if range is None:
                if isinstance(value, int):
                    return func(self, value)
                else:
                    raise ValueError(error)

            min, max = range

            if (value < min or value > max) and (value not in allowed):
                raise ValueError(error)

            return func(self, value)
        return wrapper
    return property_type_decorator


def property_matches(regex_to_match, error):

    compiled_re = re.compile(regex_to_match)

    def wrapper(func):
        @wraps(func)
        def validate_regex_decorator(self, value):
            if not compiled_re.match(value):
                raise ValueError(error)
            return func(self, value)
        return validate_regex_decorator
    return wrapper


def property_is_datetime(func):
    """ Takes the following datetime format and turns it into a datetime object:

    2016-08-18T18:25:36Z

    Because we return everything with Z as the timezone, we assume everything is in UTC and create
    a timezone aware datetime.
    """

    @wraps(func)
    def wrapper(self, value):
        if isinstance(value, datetime.datetime):
            return func(self, value)
        if not isinstance(value, basestring):
            raise ValueError("Cannot convert {} into a datetime, cannot update {}".format(value.__class__.__name__,
                                                                                          func.__name__))

        dt = parse_datetime(value)
        return func(self, dt)
    return wrapper


def property_is_data_acceleration_config(func):
    @wraps(func)
    def wrapper(self, value):
        if not isinstance(value, dict):
            raise ValueError("{} is not type 'dict', cannot update {})".format(value.__class__.__name__,
                                                                               func.__name__))
        if len(value) != 4 or not all(attr in value.keys() for attr in ('acceleration_enabled',
                                                                        'accelerate_now',
                                                                        'last_updated_at',
                                                                        'acceleration_status')):
            error = "{} should have 2 keys ".format(func.__name__)
            error += "'acceleration_enabled' and 'accelerate_now'"
            error += "instead you have {}".format(value.keys())
            raise ValueError(error)
        return func(self, value)
    return wrapper
