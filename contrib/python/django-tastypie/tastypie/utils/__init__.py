from tastypie.utils.formatting import mk_datetime, format_datetime, format_date, format_time  # noqa
from tastypie.utils.urls import trailing_slash  # noqa
from tastypie.utils.validate_jsonp import is_valid_jsonp_callback_value  # noqa
from tastypie.utils.timezone import now, make_aware, make_naive, aware_date, aware_datetime  # noqa


def string_to_python(value):
    if value in ('true', 'True', True):
        value = True
    elif value in ('false', 'False', False):
        value = False
    elif value in ('nil', 'none', 'None', None):
        value = None
    return value
