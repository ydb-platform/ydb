from sqlalchemy import types

from ..exceptions import ImproperlyConfigured
from .scalar_coercible import ScalarCoercible


class TimezoneType(ScalarCoercible, types.TypeDecorator):
    """
    TimezoneType provides a way for saving timezones objects into database.
    TimezoneType saves timezone objects as strings on the way in and converts
    them back to objects when querying the database.

    ::

        from sqlalchemy_utils import TimezoneType

        class User(Base):
            __tablename__ = 'user'

            # Pass backend='pytz' to change it to use pytz. Other values:
            # 'dateutil' (default), and 'zoneinfo'.
            timezone = sa.Column(TimezoneType(backend='pytz'))

    :param backend: Whether to use 'dateutil', 'pytz' or 'zoneinfo' for
        timezones. 'zoneinfo' uses the standard library module in Python 3.9+,
        but requires the external 'backports.zoneinfo' package for older
        Python versions.

    """

    impl = types.Unicode(50)
    python_type = None
    cache_ok = True

    def __init__(self, backend='dateutil'):
        self.backend = backend
        if backend == 'dateutil':
            try:
                from dateutil.tz import tzfile
                from dateutil.zoneinfo import get_zonefile_instance

                self.python_type = tzfile
                self._to = get_zonefile_instance().zones.get
                self._from = lambda x: str(x._filename)

            except ImportError:
                raise ImproperlyConfigured(
                    "'python-dateutil' is required to use the "
                    "'dateutil' backend for 'TimezoneType'"
                )

        elif backend == 'pytz':
            try:
                from pytz import timezone
                from pytz.tzinfo import BaseTzInfo

                self.python_type = BaseTzInfo
                self._to = timezone
                self._from = str

            except ImportError:
                raise ImproperlyConfigured(
                    "'pytz' is required to use the 'pytz' backend "
                    "for 'TimezoneType'"
                )

        elif backend == "zoneinfo":
            try:
                import zoneinfo
            except ImportError:
                try:
                    from backports import zoneinfo
                except ImportError:
                    raise ImproperlyConfigured(
                        "'backports.zoneinfo' is required to use "
                        "the 'zoneinfo' backend for 'TimezoneType'"
                        "on Python version < 3.9"
                    )

            self.python_type = zoneinfo.ZoneInfo
            self._to = zoneinfo.ZoneInfo
            self._from = str

        else:
            raise ImproperlyConfigured(
                "'pytz', 'dateutil' or 'zoneinfo' are the backends "
                "supported for 'TimezoneType'"
            )

    def _coerce(self, value):
        if value is not None and not isinstance(value, self.python_type):
            obj = self._to(value)
            if obj is None:
                raise ValueError("unknown time zone '%s'" % value)
            return obj
        return value

    def process_bind_param(self, value, dialect):
        return self._from(self._coerce(value)) if value else None

    def process_result_value(self, value, dialect):
        return self._to(value) if value else None
