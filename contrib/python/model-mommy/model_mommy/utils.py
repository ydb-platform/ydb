import importlib
import datetime
import itertools

from .timezone import tz_aware


def import_if_str(import_string_or_obj):
    """
    Import and return an object defined as import string in the form of

        path.to.module.object_name

    or just return the object if it isn't a string.
    """
    if isinstance(import_string_or_obj, str):
        return import_from_str(import_string_or_obj)
    return import_string_or_obj


def import_from_str(import_string):
    """
    Import and return an object defined as import string in the form of

        path.to.module.object_name
    """
    path, field_name = import_string.rsplit('.', 1)
    module = importlib.import_module(path)
    return getattr(module, field_name)


def seq(value, increment_by=1):
    if type(value) in [datetime.datetime, datetime.date,  datetime.time]:
        if type(value) is datetime.date:
            date = datetime.datetime.combine(value, datetime.datetime.now().time())
        elif type(value) is datetime.time:
            date = datetime.datetime.combine(datetime.date.today(), value)
        else:
            date = value
        # convert to epoch time
        start = (date - datetime.datetime(1970, 1, 1)).total_seconds()
        increment_by = increment_by.total_seconds()
        for n in itertools.count(increment_by, increment_by):
            series_date = tz_aware(datetime.datetime.utcfromtimestamp(start + n))
            if type(value) is datetime.time:
                yield series_date.time()
            elif type(value) is datetime.date:
                yield series_date.date()
            else:
                yield series_date
    else:
        for n in itertools.count(increment_by, increment_by):
            yield value + type(value)(n)
