import json
from django import template

register = template.Library()


@register.filter
def javascript_value(value):
    """
    Get javascript value for python value.

    >>> get_javascript_value(True)
    true
    >>> get_javascript_value(10)
    10
    """
    if isinstance(value, bool):
        if value:
            return "true"
        else:
            return "false"
    else:
        return json.dumps(value)
