# -*- coding: utf-8 -*-
import hashlib
from functools import wraps
import six

from .utils import registry

@registry
class Settings(object):
    """Control and configure default parsing behavior of dateparser.
    Currently, supported settings are:

    * `PREFER_DATES_FROM`: defaults to `current_period`. Options are `future` or `past`.
    * `PREFER_DAY_OF_MONTH`: defaults to `current`. Could be `first` and `last` day of month.
    * `SKIP_TOKENS`: defaults to `['t']`. Can be any string.
    * `TIMEZONE`: defaults to `UTC`. Can be timezone abbreviation or any of `tz database name as given here <https://en.wikipedia.org/wiki/List_of_tz_database_time_zones>`_.
    * `RETURN_AS_TIMEZONE_AWARE`: return tz aware datetime objects in case timezone is detected in the date string.
    * `RELATIVE_BASE`: count relative date from this base date. Should be datetime object.
    * `RETURN_TIME_AS_PERIOD`: returns period as `time` in case time component is detected in the date string.
    Default: False.
    * `PARSERS`: list of date parsers to use, in order of preference. Default:
    :attr:`dateparser.settings.default_parsers`.
    """

    _default = True
    _pyfile_data = None
    _mod_settings = dict()

    def __init__(self, settings=None):
        if settings:
            self._updateall(settings.items())
        else:
            self._updateall(self._get_settings_from_pyfile().items())

    @classmethod
    def get_key(cls, settings=None):
        if not settings:
            return 'default'

        keys = sorted(['%s-%s' % (key, str(settings[key])) for key in settings])
        return hashlib.md5(''.join(keys).encode('utf-8')).hexdigest()

    @classmethod
    def _get_settings_from_pyfile(cls):
        if not cls._pyfile_data:
            from dateparser_data import settings
            cls._pyfile_data = settings.settings
        return cls._pyfile_data

    def _updateall(self, iterable):
        for key, value in iterable:
            setattr(self, key, value)

    def replace(self, mod_settings=None, **kwds):
        for k, v in six.iteritems(kwds):
            if v is None:
                raise TypeError('Invalid {{"{}": {}}}'.format(k, v))

        for x in six.iterkeys(self._get_settings_from_pyfile()):
            kwds.setdefault(x, getattr(self, x))

        kwds['_default'] = False
        if mod_settings:
            kwds['_mod_settings'] = mod_settings

        return self.__class__(settings=kwds)


settings = Settings()


def apply_settings(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        mod_settings = kwargs.get('settings')
        kwargs['settings'] = mod_settings or settings

        if isinstance(kwargs['settings'], dict):
            kwargs['settings'] = settings.replace(mod_settings=mod_settings, **kwargs['settings'])

        if not isinstance(kwargs['settings'], Settings):
            raise TypeError(
                "settings can only be either dict or instance of Settings class")

        return f(*args, **kwargs)
    return wrapper
