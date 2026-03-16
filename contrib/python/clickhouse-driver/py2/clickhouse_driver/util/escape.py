from datetime import date, datetime
from enum import Enum
from uuid import UUID

from .compat import text_type, string_types


escape_chars_map = {
    "\b": "\\b",
    "\f": "\\f",
    "\r": "\\r",
    "\n": "\\n",
    "\t": "\\t",
    "\0": "\\0",
    "\a": "\\a",
    "\v": "\\v",
    "\\": "\\\\",
    "'": "\\'"
}


def escape_param(item):
    if item is None:
        return 'NULL'

    elif isinstance(item, datetime):
        return "'%s'" % item.strftime('%Y-%m-%d %H:%M:%S')

    elif isinstance(item, date):
        return "'%s'" % item.strftime('%Y-%m-%d')

    elif isinstance(item, string_types):
        return "'%s'" % ''.join(escape_chars_map.get(c, c) for c in item)

    elif isinstance(item, list):
        return "[%s]" % ', '.join(text_type(escape_param(x)) for x in item)

    elif isinstance(item, tuple):
        return "(%s)" % ', '.join(text_type(escape_param(x)) for x in item)

    elif isinstance(item, Enum):
        return escape_param(item.value)

    elif isinstance(item, UUID):
        return "'%s'" % str(item)

    else:
        return item


def escape_params(params):
    escaped = {}

    for key, value in params.items():
        escaped[key] = escape_param(value)

    return escaped
