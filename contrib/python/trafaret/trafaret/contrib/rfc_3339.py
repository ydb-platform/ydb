from dateutil.parser import parse
from datetime import datetime, date
from .. import Trafaret
from ..lib import STR_TYPES
from .. import codes


class DateTime(Trafaret):
    """Class for support parsing date time in RFC3339 formats
    via dateutil.parse helper
    """
    convertable = STR_TYPES + (datetime,)
    value_type = datetime

    def __init__(self, allow_blank=False):
        self.allow_blank = allow_blank

    def __repr__(self):
        return "<DateTime(blank)>" if self.allow_blank else "<DateTime>"

    def check_and_return(self, value):
        if isinstance(value, datetime):
            return value
        try:
            return parse(value)
        except (ValueError, TypeError, OverflowError) as e:
            self._failure(str(e), code=codes.NOT_DATETIME)


class Date(Trafaret):
    """Class for support parsing dates in RFC3339 formats
    via dateutil.parse helper
    """
    convertable = STR_TYPES + (date,)
    value_type = date

    def __init__(self, allow_blank=False):
        self.allow_blank = allow_blank

    def __repr__(self):
        return "<Date(blank)>" if self.allow_blank else "<Date>"

    def check_and_return(self, value):
        if isinstance(value, datetime):
            return value.date()
        elif isinstance(value, date):
            return value
        try:
            return parse(value).date()
        except (ValueError, TypeError, OverflowError) as e:
            self._failure(str(e), code=codes.NOT_DATE)
