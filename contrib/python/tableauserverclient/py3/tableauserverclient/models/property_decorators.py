import datetime
import re
from functools import wraps
from typing import Any
from collections.abc import Container

from tableauserverclient.datetime_helpers import parse_datetime


def property_is_enum(enum_type):
    def property_type_decorator(func):
        @wraps(func)
        def wrapper(self, value):
            if value is not None and not hasattr(enum_type, value):
                error = f"Invalid value: {value}. {func.__name__} must be of type {enum_type.__name__}."
                raise ValueError(error)
            return func(self, value)

        return wrapper

    return property_type_decorator


def property_is_boolean(func):
    @wraps(func)
    def wrapper(self, value):
        if not isinstance(value, bool):
            error = f"Boolean expected for {func.__name__} flag."
            raise ValueError(error)
        return func(self, value)

    return wrapper


def property_not_nullable(func):
    @wraps(func)
    def wrapper(self, value):
        if value is None:
            error = f"{func.__name__} must be defined."
            raise ValueError(error)
        return func(self, value)

    return wrapper


def property_not_empty(func):
    @wraps(func)
    def wrapper(self, value):
        if not value:
            error = f"{func.__name__} must not be empty."
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


def property_is_int(range: tuple[int, int], allowed: Container[Any] | None = None):
    """Takes a range of ints and a list of exemptions to check against
    when setting a property on a model. The range is a tuple of (min, max) and the
    allowed list (empty by default) allows values outside that range.
    This is useful for when we use sentinel values.

    Example: Revisions allow a range of 2-10000, but use -1 as a sentinel for 'unlimited'.
    """

    if allowed is None:
        allowed = ()  # Empty tuple for fast no-op testing.

    def property_type_decorator(func):
        @wraps(func)
        def wrapper(self, value):
            error = f"Invalid property defined: '{value}'. Integer value expected."

            if range is None:
                if isinstance(value, int):
                    return func(self, value)
                else:
                    raise ValueError(error)

            min, max = range
            if value in allowed:
                return func(self, value)

            if value < min or value > max:
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
    """Takes the following datetime format and turns it into a datetime object:

    2016-08-18T18:25:36Z

    Because we return everything with Z as the timezone, we assume everything is in UTC and create
    a timezone aware datetime.
    """

    @wraps(func)
    def wrapper(self, value):
        if isinstance(value, datetime.datetime):
            return func(self, value)
        if not isinstance(value, str):
            raise ValueError(
                f"Cannot convert {value.__class__.__name__} into a datetime, cannot update {func.__name__}"
            )

        dt = parse_datetime(value)
        return func(self, dt)

    return wrapper


def property_is_data_acceleration_config(func):
    @wraps(func)
    def wrapper(self, value):
        if not isinstance(value, dict):
            raise ValueError(f"{value.__class__.__name__} is not type 'dict', cannot update {func.__name__})")
        if len(value) < 2 or not all(attr in value.keys() for attr in ("acceleration_enabled", "accelerate_now")):
            error = f"{func.__name__} should have 2 keys "
            error += "'acceleration_enabled' and 'accelerate_now'"
            error += f"instead you have {value.keys()}"
            raise ValueError(error)
        return func(self, value)

    return wrapper
