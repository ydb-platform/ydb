import warnings
from datetime import datetime, timezone

from django.utils import timezone as django_timezone


def compare_states(new_state, original_state, compare_function, normalise_function):
    modified_field = {}

    for key, value in new_state.items():
        try:
            original_value = original_state[key]
        except KeyError:
            # In some situation, like deferred fields, it can happen that we try to compare the current
            # state that has some fields not present in original state because of being initially deferred.
            # We should not include them in the comparison.
            continue

        is_identical = compare_function[0](value, original_value, **compare_function[1])
        if is_identical:
            continue

        modified_field[key] = {
            'saved': normalise_function[0](original_value, **normalise_function[1]),
            'current': normalise_function[0](value, **normalise_function[1])
        }

    return modified_field


def raw_compare(new_value, old_value):
    return new_value == old_value


def timezone_support_compare(new_value, old_value, timezone_to_set=timezone.utc):

    if not (isinstance(new_value, datetime) and isinstance(old_value, datetime)):
        return raw_compare(new_value, old_value)

    db_value_is_aware = django_timezone.is_aware(old_value)
    in_memory_value_is_aware = django_timezone.is_aware(new_value)

    if db_value_is_aware == in_memory_value_is_aware:
        return raw_compare(new_value, old_value)

    if db_value_is_aware:
        # If db value is aware, it means that settings.USE_TZ=True, so we need to convert in-memory one
        warnings.warn(u"DateTimeField received a naive datetime (%s)"
                      u" while time zone support is active." % new_value,
                      RuntimeWarning)
        new_value = django_timezone.make_aware(new_value, timezone_to_set).astimezone(timezone.utc)
    else:
        # The db is not timezone aware, but the value we are passing for comparison is aware.
        warnings.warn(u"Time zone support is not active (settings.USE_TZ=False), "
                      u"and you pass a time zone aware value (%s)"
                      u" Converting database value before comparison." % new_value,
                      RuntimeWarning)
        old_value = django_timezone.make_aware(old_value, timezone.utc).astimezone(timezone_to_set)

    return raw_compare(new_value, old_value)


def normalise_value(value):
    """
    Default normalisation of value simply returns the value as is.
    Custom implementations can normalise the value for various storage schemes.
    For example, converting datetime objects to iso datetime strings in order to
    comply with JSON standard.
    """
    return value
