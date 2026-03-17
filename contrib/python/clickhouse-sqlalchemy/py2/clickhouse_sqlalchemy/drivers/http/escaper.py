from datetime import date, datetime
from decimal import Decimal
import enum

from ...util import compat


class Escaper(object):

    number_types = compat.integer_types + (float, )
    string_types = compat.string_types

    escape_chars = {
        "\b": "\\b",
        "\f": "\\f",
        "\r": "\\r",
        "\n": "\\n",
        "\t": "\\t",
        "\0": "\\0",
        "\\": "\\\\",
        "'": "\\'"
    }

    def escape_string(self, value):
        value = ''.join(self.escape_chars.get(c, c) for c in value)
        return "'" + value + "'"

    def escape(self, parameters):
        if isinstance(parameters, dict):
            return {k: self.escape_item(v) for k, v in parameters.items()}
        elif isinstance(parameters, (list, tuple)):
            return "[" + ",".join(
                [str(self.escape_item(x)) for x in parameters]) + "]"
        else:
            raise Exception("Unsupported param format: {}".format(parameters))

    def escape_number(self, item):
        return item

    def escape_date(self, item):
        # XXX: shouldn't this be `toDate(...)`?
        return self.escape_string(item.strftime('%Y-%m-%d'))

    def escape_datetime(self, item):
        # XXX: shouldn't this be `toDateTime(...)`?
        return self.escape_string(item.strftime('%Y-%m-%d %H:%M:%S'))

    def escape_datetime64(self, item):
        # XXX: shouldn't this be `toDateTime64(...)`?
        return self.escape_string(item.strftime('%Y-%m-%d %H:%M:%S.%f'))

    def escape_decimal(self, item):
        return float(item)

    def escape_item(self, item):
        if item is None:
            return 'NULL'
        elif isinstance(item, self.number_types):
            return self.escape_number(item)
        elif isinstance(item, datetime):
            return self.escape_datetime(item)
        elif isinstance(item, date):
            return self.escape_date(item)
        elif isinstance(item, Decimal):
            return self.escape_decimal(item)
        elif isinstance(item, self.string_types):
            return self.escape_string(item)
        elif isinstance(item, (list, tuple)):
            return "[" + ", ".join(
                [str(self.escape_item(x)) for x in item]
            ) + "]"
        elif isinstance(item, enum.Enum):
            return self.escape_string(item.name)
        else:
            raise Exception("Unsupported object {}".format(item))
